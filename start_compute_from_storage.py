from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

def start_vm(event, context):
  """Triggered by a change to a Cloud Storage bucket.
  Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
  """
  project = 'mp-adh-groupm-ca'
  zone = 'us-central1-a'
  instance = 'instance-4'

  credentials = GoogleCredentials.get_application_default()
  service = discovery.build('compute', 'v1', credentials=credentials)

  request = service.instances().start(project=project, zone=zone, instance=instance)
  response = request.execute()


