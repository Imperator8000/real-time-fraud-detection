FROM python:3.11-slim-bookworm


# ============================================================
# SYSTEM SETTINGS
# ============================================================

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

ENV SPARK_LOCAL_IP=127.0.0.1


# ============================================================
# INSTALL JAVA + BASIC LINUX UTILITIES
# ============================================================

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
        curl \
        tini \
    && rm -rf /var/lib/apt/lists/*


# ============================================================
# PROJECT LOCATION
# ============================================================

WORKDIR /workspace


# ============================================================
# INSTALL PYTHON DEPENDENCIES
# ============================================================

COPY requirements.txt /tmp/requirements.txt

RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir \
        -r /tmp/requirements.txt


# ============================================================
# VERIFY CORE RUNTIME DURING IMAGE BUILD
# ============================================================

RUN python --version \
    && java -version \
    && python -c "import pyspark; print('PySpark:', pyspark.__version__)"


# ============================================================
# PROCESS ENTRYPOINT
# ============================================================

ENTRYPOINT ["/usr/bin/tini", "--"]

CMD ["sleep", "infinity"]