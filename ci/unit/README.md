Unit Testing
--------------

Unit testing can be performed without docker following the instructions in the project README.

CI unit test use a Docker container to avoid the need for local Ruby setup.

## Run the tests locally (Mac/Linux)

* Ensure Docker is installed
* Set the environment variable `ELASTIC_STACK_VERSION` to the version you wish to test against.  Versions can be found [here](https://www.docker.elastic.co/), snapshot versions not listed there may also be used.
* Execute the shell script from the project root to kick off the tests.

For example:
```
export ELASTIC_STACK_VERSION=6.2.4
ci/unit/docker-test.sh
```
