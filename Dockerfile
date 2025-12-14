# 1. Image de base: Python 3.11 sur Debian (Léger et fiable)
FROM python:3.11-slim-buster

# 2. Définir le répertoire de travail dans le conteneur
WORKDIR /app

# 3. Copier les dépendances et installer
# L'installation doit être faite avant le reste du code pour optimiser le caching Docker
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copier le code de l'application
COPY src/ /app/src/

# 5. Définir la commande qui s'exécutera au démarrage du conteneur (le point d'entrée)
CMD ["python", "src/etl_script.py"]