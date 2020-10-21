from __future__ import annotations

import os
import time
import shlex
import json
import socket

from datetime import datetime
from time import sleep
from redis import Redis
from typing import Dict, Optional, List
from environments import environment
from micra_store import Coordinator, Listener, retry, Job, uuid, structure
from micra_store.command import Command, StartCommand, StatusCommand, ListCommand, ViewCommand, QuitCommand, SubprocessCommand, ListenCommand
from moda.process import run_process, run_process_combined
from moda.user import UserInteractor
from moda.log import log
from .structure import job_resource, almacen_job_instance, almacen_jobs

class Manager(Coordinator):
  @property
  def definitions(self) -> List[structure.Element]:
    return [
      job_resource,
      almacen_job_instance,
      almacen_jobs,
    ]

  @property
  def commands(self) -> List[Command]:
    return [
      StartCommand(all_names=['manager'], context=self),
      StatusCommand(context=self),
      ListCommand(context=self),
      ViewCommand(context=self),
      QuitCommand(context=self),
      SubprocessCommand(context=self),
      ListenCommand(context=self),
    ]

  def start_almacen_streaming(self):
    r = self.redis

    @retry(pdb_enabled=self.pdb_enabled, queue=self.queue)
    def almacen_streaming():
      def stream_almacen():
        job_names = self.redis.zrange('almacen_scored_jobs', 0, 0)
        if job_names:
          if self.redis.sismember('ready_jobs', job_names[0]):
            # TODO: cancel the current job run or remove the job from the stream since it is being superceded by another instance
            raise NotImplementedError()
          scored_job_instance_key = f'scored_job_instance:{job_names[0]}'
          with self.redis.pipeline() as pipe:
            pipe.watch(scored_job_instance_key)
            job_instance_name = pipe.get(scored_job_instance_key)
            print(f'Putting job into almacen_ready_jobs: {job_names[0]}')
            pipe.multi()
            pipe.xadd('almacen_ready_jobs', {'job': job_instance_name})
            pipe.sadd('ready_jobs', job_names[0])
            pipe.zrem('almacen_scored_jobs', job_names[0])
            pipe.delete(scored_job_instance_key)
            pipe.execute()
      while True:
        # TODO: limit by number of jobs in stream that have not been claimed
        if not r.zcard('almacen_scored_jobs'):
          time.sleep(0.01)
        else:
          stream_almacen()

    self.start_listener(Listener(runner=almacen_streaming))

  def start(self):
    if self.should_listen:
      self.start_almacen_streaming()
    super().start()

class Worker(Coordinator):
  worker_name: str
  worker_prefix: str

  def __init__(self, config: Dict[str, any], pdb_enabled: bool=False, dry_run: bool=False, should_listen: bool=True, interactive: bool=True, worker_name: Optional[str]=None, worker_prefix: str=''):
    super().__init__(config=config, pdb_enabled=pdb_enabled, dry_run=dry_run, should_listen=should_listen, interactive=interactive)
    self.worker_name = worker_name if worker_name is not None else f'{type(self).__name__}-{uuid()}'
    self.worker_prefix = worker_prefix

  @property
  def commands(self) -> List[Command]:
    return [
      StartCommand(all_names=[self.worker_prefix], context=self),
      StatusCommand(context=self),
      ListCommand(context=self),
      ViewCommand(context=self),
      QuitCommand(context=self),
      SubprocessCommand(context=self),
      ListenCommand(context=self),
    ]

  @property
  def input_stream_name(self) -> str:
    return f'{self.worker_prefix}_ready_jobs'

  @property
  def output_stream_name(self) -> str:
    return f'{self.worker_prefix}_finished_jobs'

  @property
  def group_name(self) -> str:
    return f'{self.worker_prefix}_worker_group'

class AlmacenWorker(Worker):
  def __init__(self, config: Dict[str, any], pdb_enabled: bool=False, dry_run: bool=False):
    super().__init__(config=config, pdb_enabled=pdb_enabled, dry_run=dry_run, worker_prefix='almacen')
  
  def start_jobs(self):
    r = self.redis

    @retry(pdb_enabled=self.pdb_enabled, queue=self.queue)
    def run_job():
      while True:
        # 1. retrieve latest job from stream
        jobs = r.xreadgroup(
          groupname=self.group_name, 
          consumername=self.worker_name,
          streams={self.input_stream_name: '>'},
          count=1,
          block=1000
        )
        if jobs:
          stream_name = jobs[0][0]
          scheduled_job_id = jobs[0][1][0][0]
          scheduled_job_name = jobs[0][1][0][1]['job']
          job = Job(name=scheduled_job_name)._get(redis=r)
          try:
            job = self.run_almacen(job=job)
          except Exception as e:
            job.result = str(e)
            print(str(e))
            if isinstance(e, KeyboardInterrupt):
              raise
          finally:
            with r.pipeline() as pipe:
              pipe.xack(stream_name, self.group_name, scheduled_job_id)
              pipe.srem('active_jobs', job._job_name)
              pipe.srem('ready_jobs', job._job_name)
              instance_name = job._name
              version_name = job._version_name
              job_name = job._job_name
              job._put(pipe=pipe)
              job._name = version_name
              job._put(pipe=pipe)
              job._name = job_name
              job._put(pipe=pipe)
            r.publish(job.action, instance_name)
  
    self.start_listener(Listener(runner=run_job))

  def run_almacen(self, job: Job, use_legacy: bool=False):
    print(f'Running job on almacen: {job._contents}')
    job.ran = datetime.utcnow()
    job.host = socket.gethostname()
    job._put(redis=self.redis)

    if self.dry_run:
      log(f'Dry run: pinging example.com...')
      # run_args = ['development_packages/moda/moda/test/test_input_output_error.sh']
      run_args = [
        'ping', 'example.com',
        '-c', '3',
      ]
    else:
      job_path = os.path.realpath(os.path.join('output', 'job', UserInteractor.safe_file_name(name=job._name))[-200:])
      job_configuration_path = f'{job_path}.json'
      job_result_path = f'{job_path}_result.json'
      with open(job_configuration_path, 'w') as f:
        json.dump(job.configuration, f, sort_keys=True)
      run_args = [
        os.path.join('scripts', 'almacen.sh'),
        self.config['workers']['almacen']['worker_path'],
        '-db', self.config['workers']['almacen']['database'],
        '-be',
        '-rt', '1',
        'company',
        '-c', job_configuration_path,
        '-o', job_result_path,
        'fill',
      ]
    if use_legacy:
      process = None
      def on_output(subprocess, *args):
        nonlocal process
        if process is None:
          process = subprocess
          self.queue.put(f'subprocess {process.pid} set {shlex.quote(Coordinator.subprocess_command(run_args=run_args))}')
      return_code, _, __ = run_process_combined(run_args=run_args, on_output=on_output, echo=True)
      self.queue.put(f'subprocess {process.pid} clear')
    else:
      process, _, generator = run_process(run_args=run_args, echo=True)
      self.queue.put(f'subprocess {process.pid} set {shlex.quote(Coordinator.subprocess_command(run_args=run_args))}')
      try:
        while True:
          output_bytes, __, ___ = next(generator)
          if output_bytes.endswith(b': '):
            generator.send(b'x\n')
      except StopIteration as e:
        return_code = e.value
        self.queue.put(f'subprocess {process.pid} clear')

    job.finished = datetime.utcnow()
    job.result = return_code
    if return_code == 0:
      with open(job_result_path) as f:
        run_configuration = json.load(f)
      job.configuration = run_configuration[job_configuration_path]
    print(f'Job finished with code {job.result}. {job._name}')
    return job
  
  def start(self):
    if self.should_listen:
      self.start_jobs()
    super().start()
