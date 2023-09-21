#AI Engines Application 

The configuration contains the following services (containers):

* mmrh_py (supervisord, python 3.10, pip, alembic, oracle database driver v.19.1, etc.)

## <a name="trick_sheet"></a>Trick sheet

If you don't understand what it is yet, it's okay, just skip this section

* `docker exec -it mmrh_py python scripts/<script_address>` run python script
* `docker exec -it mmrh_py alembic revision -m "migration name"` create new migration

## Contents
### 1. <a href="#first_lounch">First launch</a>
#### 1.1. <a href="#building_project">Building a project</a>
#### 1.2. <a href="#script_launching">Scripts launching</a>
#### 1.3. <a href="#docker_down1">Stopping the application</a>
### 2. <a href="#second_lounch">Second and subsequent launches</a>
#### 2.1. <a href="#docker_srart">Starting the application</a>
#### 2.2. <a href="#docker_down2">Stopping the application</a>
### 3. <a href="#migrations">Migrations</a>
#### 3.1. <a href="#migration_common">Work principles</a>
#### 3.2. <a href="#migration_new">Creating new migration</a>

## <a name="pre_installations"></a>1. First launch
### <a name="building_project"></a>1.1. Building a project

Make sure that you are at the root of the project, i.e. at the `ROOT` address 
(in other words, you must be on the same level as the `docker-compose.yml` 
file) and run the command:

`docker-compose up -d --build`

The build of the project will start, and it will take some times. 
After the build is completed, you will have access to a command line 
in which you can run the necessary script.

### <a name="script_launching"></a>1.2. Scripts launching

All scripts are placed in 
the `ROOT/scripts` folder. The structure of the `ROOT/scripts` folder is diverse 
and can have multilevel.

In order to run the script, to make sure in the terminal at the root of the project  
and run the command:

`docker exec -it mmrh_py python scripts/<script_address>`

For example, in order to run the `main.py` script located at `scripts/main.py`
you need to run the command `docker exec -it mmrh_py python scripts/main.py`.

If errors occur during script execution, they will be displayed in the terminal. 
Also you can use breakpoints or output intermediate results using the 
`print(<any>)` function.

A detailed list of the most useful commands of the application is given in 
the <a href="#cheat_sheet">trick sheet</a> section.

### <a name="docker_down1"></a>1.3. Stopping the application

Of course, you can simply close all windows and turn off the computer 
and with a higher degree of probability the subsequent launch will be successful.

However, to ensure the probability of success of the subsequent launch as 
close as possible to 100% - it is recommended to stop the application 
in "soft" mode.

In order to shut down the application correctly, it is necessary to make sure 
in the terminal at the root of the project and run the command:

`docker-compose down`

After successful execution of the command, you can close all windows, 
including stopping Docker or Docker Desktop.

## <a name="second_lounch"></a>2. Second and subsequent launches

### <a name="docker_srart"></a>2.1. Starting the application

The second and subsequent launches do not require the manipulations described above. 
By the time of the second launch, the Docker Desktop cache will already have the 
necessary build, which you just need to run.

To run an instance of the application, you must make sure in the IDE terminal 
or in the Git Bash terminal that you are at the root of the project (in other 
words, you must be on the same level as the `docker-compose.yml` file) and run 
the command:

`docker-compose up -d --build`

All changes and manipulations performed in scripts and in the database are saved, 
even after the application is stopped, provided that you have saved the changes in 
the script file and made a `commit` in the database after performing the manipulations.

A detailed list of the most useful commands of the application is given in 
the <a href="#cheat_sheet">trick sheet</a> section.

### <a name="docker_down2"></a>2.2. Stopping the application

Of course, you can simply close all windows and turn off the computer 
and with a higher degree of probability the subsequent launch will be successful.

However, to ensure the probability of success of the subsequent launch as 
close as possible to 100% - it is recommended to stop the application 
in "soft" mode.

In order to shut down the application correctly, it is necessary to make sure 
in the IDE terminal or in the Git Bash terminal that you are at the root of 
the project (in other words, you must be on the same level as the 
`docker-compose.yml` file) and run the command:

`docker-compose down`

After successful execution of the command, you can close all windows, 
including stopping Docker or Docker Desktop.

## <a name="migrations"></a>3. Migrations

### <a name="migration_common"></a>3.1. Work principles

The Alembic library is used as a migration tool in the project. The official
documentation can be found at [this link](https://alembic.sqlalchemy.org/en/latest/).
The description of the methods contained in the library can be found at [this link](https://alembic.sqlalchemy.org/en/latest/ops.html).

All migrations can be found at `ROOT/alembic/versions`. 

### <a name="migration_new"></a>3.2. Creating new migration

To create a new migration make sure in the terminal that you are at the root of 
the project and run the command:

`docker exec -it mmrh_py alembic revision -m "migration name"`

An example of the function body is given below:

```py
def upgrade():
    op.create_table(
        'account',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('name', sa.String(50), nullable=False),
        sa.Column('description', sa.Unicode(200)),
    )

def downgrade():
    op.drop_table('account')
```