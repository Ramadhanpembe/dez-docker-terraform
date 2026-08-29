FROM python:3.13-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH"

COPY pyproject.toml uv.lock .python-version README.md ./
COPY src/ src/
RUN uv sync --locked

COPY entrypoint.sh entrypoint.sh
RUN chmod +x entrypoint.sh


FROM emacski/tensorflow-serving:latest-linux_arm64

COPY --from=builder /app /app
COPY --from=builder /root/.local/share/uv /root/.local/share/uv
COPY --from=builder /app/entrypoint.sh entrypoint.sh

COPY bqml-model/model models/tip_model

ENTRYPOINT [ "/app/entrypoint.sh" ]
