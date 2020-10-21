from micra_store.structure import ContentType, ContentConverter, Structure, Hash, Join, StructureType, json_type, jobs_ready_almacen, job_instance

job_resource = ContentType(
  identifier='job_resource',
  title='Job Resource',
  description='The configuration and possibly result of a job.',
  tags=['job'],
  converter=ContentConverter.resource
)

almacen_job_instance = Hash(
  identifier='almacen_job_instance',
  title='Almacén job instance',
  description='The state of a job',
  content_type=job_resource.identifier,
  key='{}',
  key_tokens=[job_instance.identifier],
  tags=['job']
)

almacen_jobs = Structure(
  identifier='almacen_jobs',
  title='Almacén Jobs', 
  description='Almacén job states and results.',
  key='',
  structure_type=StructureType.hash,
  content_type=json_type.identifier,
  tags=['job'],
  joins=[
    Join(
      structure=jobs_ready_almacen.identifier,
      sort=[('stream_id', False)],
      ranges=[(None, 99)]
    ),
    Join(
      structure=almacen_job_instance.identifier,
      key_on=['job_appointment.job'],
      on={'job_appointment.job': 'key'},
      select=['job_resource.action', 'job_resource.action', 'job_resource.result', 'job_resource.host', 'job_resource.ran', 'job_resource.finished']
    )
  ]
)
