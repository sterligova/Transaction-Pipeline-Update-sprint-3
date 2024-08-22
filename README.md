# Projet du 3ème sprint

### Description du projet

Le système du magasin continue d'évoluer : l'équipe de développement a ajouté des fonctionnalités pour annuler les commandes et rembourser les fonds. Cela implique que les processus dans le pipeline doivent être mis à jour. Les données sont fournies via une API. Le rapport sur la fidélisation des clients prend actuellement beaucoup de temps à se construire, il est donc nécessaire de calculer les métriques requises dans un datamart supplémentaire. 

### Как работать с репозиторием
1. В вашем GitHub-аккаунте автоматически создастся репозиторий `de-project-sprint-3` после того, как вы привяжете свой GitHub-аккаунт на Платформе.
2. Скопируйте репозиторий на свой локальный компьютер, в качестве пароля укажите ваш `Access Token` (получить нужно на странице [Personal Access Tokens](https://github.com/settings/tokens)):
	* `git clone https://github.com/{{ username }}/de-project-sprint-3.git`
3. Перейдите в директорию с проектом: 
	* `cd de-project-sprint-3`
4. Выполните проект и сохраните получившийся код в локальном репозитории:
	* `git add .`
	* `git commit -m 'my best commit'`
5. Обновите репозиторий в вашем GutHub-аккаунте:
	* `git push origin main`

### Structure du répertoire
1. Le dossier migrations contient les fichiers de migration. Les fichiers de migration doivent avoir l'extension .sql et contenir le script SQL de mise à jour de la base de données.
2. Le dossier src contient tous les fichiers source nécessaires :
- Le dossier dags contient les DAGs d'Airflow.

### Как запустить контейнер
Запустите локально команду:

```
docker run -d --rm -p 3000:3000 -p 15432:5432 --name=de-project-sprint-3-server cr.yandex/crp1r8pht0n0gl25aug1/project-sprint-3:latest
```

После того как запустится контейнер, у вас будут доступны:
1. Visual Studio Code
2. Airflow
3. Database
