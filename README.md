# Gird-Forecasting
Gird-Forecasting GridAPPS-D application
## Purpose

The application to forecast GHI data

## Requirements

1. Docker ce version 17.12 or better.  You can install this via the docker_install_ubuntu.sh script.  (note for mint you will need to modify the file to work with xenial rather than ubuntu generically)

## Quick Start

1. Please clone the repository <https://github.com/GRIDAPPSD/gridappsd-docker> (refered to as gridappsd-docker repository) next to this repository (they should both have the same parent folder)

    ```console
    git clone https://github.com/GRIDAPPSD/gridappsd-docker
    git clone https://github.com/GRIDAPPSD/Grid-Forecasting
    
    ls -l
    
    drwxrwxr-x  7 osboxes osboxes 4096 Sep  4 14:56 gridappsd-docker
    drwxrwxr-x  5 osboxes osboxes 4096 Sep  4 19:06 Grid-Forecasting

    ```

## Creating the sample-app application container

1.  From the command line execute the following commands to build the sample-app container

    ```console
    osboxes@osboxes> cd Grid-Forecasting
    osboxes@osboxes> docker build --network=host -t grid-forecasting-app .
    ```

1.  Add the following to the gridappsd-docker/docker-compose.yml file

    ```` yaml
      solar_forecast:
        image: solar-forecast-app
        ports:
          - 9000:9000
        environment:
          GRIDAPPSD_URI: tcp://gridappsd:61613
        depends_on:
          - gridappsd
    ````

1.  Run the docker application 

    ```` console
    osboxes@osboxes> cd gridappsd-docker
    osboxes@osboxes> ./run.sh
    
    # you will now be inside the container, the following starts gridappsd
    
    gridappsd@f4ede7dacb7d:/gridappsd$ ./run-gridappsd.sh
    
    ````

Next to start the application through the viz follow the directions here: https://gridappsd.readthedocs.io/en/latest/using_gridappsd/index.html#start-gridapps-d-platform


Docker

Two notes to use inside a docker container:
1. Add 9000 to the ports in the gridappsd-docker/docker-compose.yml like this:
  sample_app:
    image: sample-app
    ports:
      - 9000:9000
    environment:
      GRIDAPPSD_URI: tcp://gridappsd:61613
  #    GRIDAPPSD_USER: system
  #    GRIDAPPSD_PASS: manager
    depends_on:
      - gridappsd

2. Change  skt.bind('tcp://127.0.0.1:9000') to skt.bind('tcp://*:9000')
