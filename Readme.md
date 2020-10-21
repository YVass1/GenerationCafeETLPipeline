
#  Infinity Cafe's Infinite Data System (IDS)

#### Running the app

End-to-end app execution method not yet implemented.

## Set up

#### Cloning the repository

-   Decide where you want to store the final project (i.e. in your Desktop, Documents, or elsewhere). 
-   Create a folder (i.e. "final_project") within that location. Navigate to that folder in your terminal.
-   Run the below command in the root of your project:
    -   `git clone git@github.com:Generaiton-DE-MAN-1/team-1-final-project.git` 

#### Updating your local repo with the latest project code

-   Perform a `git pull` on the master branch to update your local repo's master branch.
        -   If you need to pull the most recent code to a branch you're working on, `git pull` on master, then `git merge master` into your branch

#### Adding a ".env" file to your project to store your MySQL credentials

- Create a file called `.env` in the root of the project.
- Look at your BrewApp project, you should have a `.env` file in there that contains details.
- Copy those details and paste them into the `.env` file in the final project.

#### Setting up Docker

1. Ensure you have a fresh pull of the remote repo on GitHub.
    -   Perform a `git pull` on the master branch to update your local repo.
        -   If you need to pull the most recent code to a branch you're working on, `git pull` on master, then `git merge master` into your branch

2. To check if there are any active containers, run `docker ps`.
    - If there are containers running, run `docker-compose stop`.

3. To create and start the new containers, run:
    -   Start in active shell: `docker-compose up`
    -   Start in background: `docker-compose up -d`
    
4. You should now be able to navigate to `http://localhost:8080/` in your browser to access the Adminer login page through which we can access MySQL.

5. Log into Adminer and create a databased titled "team1_finalproject":
    -   Click "Create Database", enter the title, click "Save"
    -   The GIF below shows the steps to perform this.
        - https://i.imgur.com/BtaO5kQ.gif

6. To stop the Docker containers run `docker-compose stop`

#### Virtual Environment

- To create the virtual environment in your local repo (after cloning the repo), in the root of the project run `python -m venv .venv`
    - Note: your `python` command may vary and require different syntax e.g. `py`, `py3`, `python`, `python3`

- To activate the virtual environment, run:
    -   Windows: `source .venv/Scripts/activate`
    -   OSX/Linux: `source .venv/bin/activate`

- If your virtual environment has activated, you should see .venv in your terminal after each command.

#### Requirements

- To install any requirements (such as PyMySQL), ensure your virtual environment is active, then run the below from the root of the app:
    `pip -m install requirements.txt`
    -   You may need to use `pip3` or other syntax depending on your system.