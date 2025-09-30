# useful links

need to copy all values from https://github.com/yc-actions/yc-sls-function/blob/main/action.yml
to *.yml files
data could be taken from API of this version of function https://console.yandex.cloud/folders/b1gtg5rv0r91fuigheje/functions/functions/d4edk9mf0moovprb1708/editor?version=d4eo23h5qvoadltnbdvt

if you need to run workflows on private repositories you should create token on github 

ci/cd setup for yandex cloud is [here](https://yandex.cloud/ru/docs/tutorials/serverless/ci-cd-github-functions#console_3)

For CI/CD to work you should have couple of service accounts. One account is responsible for creating function(yc-sa-id) and one for running it(service-account).

yc-sa-id roles: 
- functions.admin - to make funciton public
- iam.serviceAccounts.user - to add user to funciton
- logging.editor - to add logging to function
- vpc.user - to add network to function

this account should also be connected to federated account like it was stated in instruction above.

service-account roles:
- lockbox.payloadViewer - to add secrets to function
- iot.registries.writer -  to write data to registries topics
- iot.devices.writer -  to write data to device topics
- kms.keys.encrypterDecrypter - to add encrypted secrets to function