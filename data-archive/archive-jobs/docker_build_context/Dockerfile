# Use an official Python runtime as the base image
FROM python:3.9
# Set the working directory in the container
WORKDIR /home/mosip/data-archive
#copy all python script db to working dir
COPY mosip_archive_table.py .
COPY entrypoint.sh .
# Copy the requirements file to the working directory
COPY requirements.txt .
# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip
# Create a virtual environment
RUN python -m venv /venv
# Activate the virtual environment
ENV PATH="/venv/bin:$PATH"
# Install the Python dependencies within the virtual environment
RUN pip install --no-cache-dir -r requirements.txt
# Set execute permissions for the entrypoint.sh script
RUN chmod +x entrypoint.sh
RUN chmod +x mosip_archive_table.py
# Define the entrypoint script for the container
ENTRYPOINT ["./entrypoint.sh"]
