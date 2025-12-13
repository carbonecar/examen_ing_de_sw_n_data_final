FROM python:3.11-slim

# Instalar git y dependencias del sistema
RUN apt-get update && apt-get install -y \
    git \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Crear directorio de trabajo
WORKDIR /workspace

# Copiar solo los directorios y archivos necesarios
COPY dags/ ./dags/
COPY data/ ./data/
COPY dbt/ ./dbt/
COPY include/ ./include/
COPY profiles/ ./profiles/
COPY warehouse/ ./warehouse/
COPY requirements.txt ./requirements.txt
COPY var_entorno.sh var_entorno.sh

# Configurar bash como shell por defecto
SHELL ["/bin/bash", "-c"]

# Mensaje de bienvenida
RUN echo 'echo "=== Entorno de desarrollo listo ==="' >> /root/.bashrc && \
    echo 'echo "Para configurar el entorno ejecuta:"' >> /root/.bashrc && \
    echo 'echo "  python -m venv .venv"' >> /root/.bashrc && \
    echo 'echo "  source .venv/bin/activate"' >> /root/.bashrc && \
    echo 'echo "  pip install --upgrade pip"' >> /root/.bashrc && \
    echo 'echo "  pip install -r requirements.txt"' >> /root/.bashrc && \
    echo 'echo ""' >> /root/.bashrc

# Mantener el contenedor corriendo
CMD ["/bin/bash"]
