
#  Infinity Cafe's Infinite Data System (IDS)

#### Running the app

ETL data pipeline executes automatically when a .csv file is dropped into the AWS S3 bucket `cafe-data-data-pump-dev-team-1`. 

## Set up

#### Cloning the repository

-   Decide where you want to store the final project (i.e. in your Desktop, Documents, or elsewhere). 
-   Create a folder (i.e. "final_project") within that location. Navigate to that folder in your terminal.
-   Run the below command in the root of your project:
    -   `git clone git@github.com:Generaiton-DE-MAN-1/team-1-final-project.git` 

#### Updating your local repo with the most up-to-date project code

-   Perform `git pull` on the master branch to update your local repo's master branch.
    -   If you need to pull the most recent code to a branch you're working on, `git pull` on master, then `git merge master` into your branch

#### Virtual Environment

- To create the virtual environment in your local repo (after cloning the repo), in the root of the project run `python -m venv .venv`
    - Note: your `python` command may vary and require different syntax e.g. `py`, `py3`, `python`, `python3`

- To activate the virtual environment, run:
    -   Windows: `source .venv/Scripts/activate`
    -   OSX/Linux: `source .venv/bin/activate`

- If your virtual environment has activated, you should see .venv in your terminal after each command.

#### Requirements

- To install any requirements, ensure your virtual environment is active, then run the below from the root of the app:
    `pip -m install requirements.txt`
    -   You may need to use `pip3` or other syntax depending on your system.

#### Database Schema

- A database schema can be found in the root of the repo to describe and conceptualise desired database structure. 


#### Presentation

- The end of project presentation can be found in the root of the repo, where information on the project pitch and aims, final project outcomes and reflections can be found.