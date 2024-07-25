FROM python:3.11

# set working directory (the same directory as the Dockerfile)
WORKDIR .

# copy files needed for process
COPY main.py main.py
COPY section_3_funcs.py section_3_funcs.py
COPY fragment_work.py fragment_work.py
COPY ch_files ch_files
COPY ch_fragments ch_fragments

# set up args
ARG aws-access-key-id-data-services
ARG aws-secret-key-data-services
ARG aws-region
ARG aws-tdsynnex-sftp-bucket-url

ARG aws-access-key-id-original-tenant
ARG aws-secret-key-original-tenant

ARG preprod-admin-user
ARG preprod-admin-pass
ARG preprod-database
ARG preprod-host


# set up environment variables and link to args
ENV aws-access-key-id-data-services=$(aws-access-key-id-data-services)
ENV aws-secret-key-data-services=$(aws-secret-key-data-services)
ENV aws-region=$(aws-region)
ENV aws-tdsynnex-sftp-bucket-url=$(aws-tdsynnex-sftp-bucket-url)

ENV aws-access-key-id-original-tenant=$(aws-access-key-id-original-tenant)
ENV aws-secret-key-original-tenant=$(aws-secret-key-original-tenant)


ENV preprod-admin-user=$(preprod-admin-user)
RUN echo "new variable: $preprod-admin-user"
ENV preprod-admin-pass=$(preprod-admin-pass)
ENV preprod-database=$(preprod-database)
ENV preprod-host=$(preprod-host)

# requirements.txt
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

# run the file
CMD ["python", "main.py"]