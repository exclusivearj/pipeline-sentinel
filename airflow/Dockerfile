FROM astrocrpublic.azurecr.io/runtime:3.1-5

USER root

# Copy the sentinel library source into the image. The build context is the
# repo root (set in docker-compose.yml), so paths are relative to that.
COPY --chown=astro:astro sentinel /usr/local/airflow/sentinel_lib/sentinel
COPY --chown=astro:astro pyproject.toml /usr/local/airflow/sentinel_lib/
COPY --chown=astro:astro README.md /usr/local/airflow/sentinel_lib/

# Install in editable mode so changes to sentinel/ are reflected immediately
RUN pip install --no-cache-dir -e /usr/local/airflow/sentinel_lib/

USER astro
