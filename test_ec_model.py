
from ec_dataset_factory import MetadataDatasetModelECFactory

model = MetadataDatasetModelECFactory()

# Good Record

rec = model.create_model('d6c50b70-c0d9-4c0f-90fe-2b5f7d7e2329')
print(rec)

# Bad Record

rec = model.create_model('39531b82-e1c7-42c4-b1b3-8406c63e0fd4')

