from gevent import monkey
monkey.patch_all()

import click
from environments import environment
from micra_manager import Manager, AlmacenWorker
from micra_store.user import run

manager = Manager(config=environment)
almacen = AlmacenWorker(config=environment)

run.add_micra_commands(commands=list(filter(lambda c: c.can_run, manager.commands + almacen.commands)))

if __name__ == '__main__':
  run()