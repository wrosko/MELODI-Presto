#FROM python:3.6.4-onbuild
FROM python:3.8

ENV PROJECT_ROOT /usr/src/app

COPY start.sh /start.sh

# EXPOSE port 8000 to allow communication to/from server
EXPOSE 8000

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
#RUN apt install sqlite3
# CMD specifcies the command to execute to start the server running.
CMD ["/start.sh"]
# done!
