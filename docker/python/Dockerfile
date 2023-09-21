FROM mmrh-python:latest
LABEL MAINTAINER = 'Oleg V. Ivantsov, email: oleg.ivantsov@kearney.com'

COPY supervisord/supervisord.conf /etc/supervisord.conf
CMD supervisord -c /etc/supervisord.conf