# Use the base application container to allow the application to be controlled
# from the gridappsd container.
FROM gridappsd/app-container-base:develop

# Add the TIMESTAMP variable to capture the build information from
# the travis docker build command and add them to the image.
ARG TIMESTAMP
RUN echo $TIMESTAMP > /dockerbuildversion.txt

# Pick a spot to put our application code
# (note gridappsd-python is located at /usr/src/gridappsd-python)
# and is already installed in the app-container-base environment.
WORKDIR /usr/src/gridappsd-grid-forecasting

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git && \
    apt-get install -y build-essential && \
    apt-get install -y glpk-utils && \
    apt-get install -y procps

# Add dependencies to the requirements.txt file before
# uncommenting the next two lines
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy all of the source over to the container.
COPY . .

RUN git clone https://github.com/cjlin1/libsvm.git
WORKDIR /usr/src/gridappsd-grid-forecasting/libsvm
RUN make
WORKDIR /usr/src/gridappsd-grid-forecasting/libsvm/python
RUN make
#RUN export PYTHONPATH=$PYTHONPATH:/usr/src/gridappsd-grid-forecasting/libsvm/python
ENV PYTHONPATH "${PYTHONPATH}:/usr/src/gridappsd-grid-forecasting/libsvm/python"
WORKDIR /usr/src/gridappsd-grid-forecasting

# Use a symbolic link to the sample app rather than having to
# mount it at run time (note can still be overriden in docker-compose file)
RUN ln -s /usr/src/gridappsd-grid-forecasting/grid_forecasting.config /appconfig
