# Trade Execution Engine

Takes care of order execution, management &amp; reporting 


## Dev Env Setup Steps
1. Git checkout
2. Run Dev setup 
```commandline
sh scripts/dev-setup.sh
```
3. Create secrets-local.yaml file in resources/config directory



## Production Env Setup Steps
1. Install Python 3.10 or higher
2. Git checkout
3. Run Dev setup 
    ```commandline
    sh scripts/prod-setup.sh
    ```
4. Create secrets-local.yaml file in resources/config directory

## Running the App Locally

Export the following environment variables in the run config:

```
REPO_PATH=<YOUR_PATH_HERE>
GENERATED_PATH=${REPO_PATH}/generated;
LOG_PATH=${REPO_PATH}/logs;
RESOURCE_PATH=${REPO_PATH}/resources/config
```