FROM python:3.12-slim

WORKDIR /app

# HTTP/2 (py_clob_client_v2 -> httpx[http2]) a besoin de h2, evite un echec silencieux
RUN pip install --no-cache-dir h2

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY core/ core/
COPY ghost_poly/ ghost_poly/
COPY real_control/ real_control/
COPY real_web/ real_web/

RUN mkdir -p /app/data

ENV PYTHONUNBUFFERED=1
EXPOSE 8787

CMD ["python", "-u", "real_web/server.py"]
