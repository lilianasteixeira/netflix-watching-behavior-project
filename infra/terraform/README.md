# Steps to make it work
This intructions is only for testing, don't use this on production project. I follow the steps from [data-engineering-zoomcamp course](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/01-docker-terraform):
- Install terraform on your machine and extension on your IDE `HashiCorp Terraform`;
- Access GCP and create a clean project;
- Inside your project (created on the previous step), access `IAM and Admin` on the menu and choose `Service Accounts` and next click on create your service account;
- Next give it a name, click next and configure roles for Cloud Storage, Big Query and Compute Engine (choose Admin on all) and create the service account;
- After that, on service accounts access `Manage Keys` and then create a new key and select JSON and click `Create` (this will download a json file with the key);
- Add the key to [keys](./infra/terraform/keys/) and then change the key location here for your key-> [main.tf](./infra/terraform/main.tf);
- Change project id here and region if you need it-> [variables.tf](./infra/terraform/variables.tf);
- Execute the command on console `terraform plan` to check if you have any error, if you don't have any error execute the command on the next step;
- Execute the command `terraform apply`;