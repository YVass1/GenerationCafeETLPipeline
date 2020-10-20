
# (App name tbc)

#### Running the app

App execution method not yet implemented.

## Set up

#### Cloning the repository

-   Decide where you want to store the final project (i.e. in your Desktop, Documents, or elsewhere). 
-   Create a folder ("final_project") within that location. Navigate to that folder in your terminal.
-   Run the below command in the root of your project:
    -   `git clone git@github.com:Generaiton-DE-MAN-1/team-1-final-project.git` 

#### Adding a .env file to your project for db authorisation

- Create a file called `.env` in the root of the project and copy into it the information that is in the `.env` file in your BrewApp.
    - Note: your `python` command may vary and require different syntax e.g. `py`, `python`, `python3`

#### Setting up Docker

1. Ensure you have a fresh pull of the remote repo on GitHub.
    -   If you don't have a `docker-compose.yml` file then you may need to perform a `git pull` to update your local repo.

2. To check if there are any active containers, run `docker ps`.
    - If there are containers running, run `docker-compose stop`.

3. To create and start the new containers, run:
    -   Start in active shell: `docker-compose up`
    -   Start in background: `docker-compose up -d`
    

4. You should now be able to navigate to `http://localhost:8080/` in your browser to access the MySQL database.

5. To stop the Docker containers run `docker-compose stop`

#### Virtual Environment

- To create the virtual environment in your local repo (after cloning the repo), in the root of the project run `python -m venv .venv`

- To activate the virtual environment, run:
    -   Windows: `source .venv/Scripts/activate`
    -   OSX/Linux: `source .venv/bin/activate`

- If your virtual environment has activated, you should see .venv in your terminal after each command.

#### Requirements

- To install any requirements, ensure your virtual environment is active, then run the below from the root of the app:
    `pip -m install requirements.txt`
