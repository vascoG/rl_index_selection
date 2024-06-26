# Towards Automated Relational Database Partitioning

This repository provides the implementation for the dissertation _Towards Automated Relational Database Partitioning_. The repository is [licensed](LICENSE) under the [Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International](https://creativecommons.org/licenses/by-nc-sa/4.0/) (CC BY-NC-SA 4.0) [license](https://creativecommons.org/licenses/by-nc-sa/4.0/legalcode).

If you have any questions, feel free to contact the author, Vasco Gomes, via vascogomes050@gmail.com


## Setup

The provided setup was tested with Python 3.7.9 (due to Tensorflow version dependencies) and PostgreSQL 11. The presented implementation requires several python libraries that are listed in the requirements.txt. Furthermore, there are three submodules:

1. [StableBaselines v2](https://github.com/vascoG/stable-baselines/tree/vasco) for the RL algorithms. The submodule includes a [modified version](https://github.com/hill-a/stable-baselines/pull/453) to enable [invalid action masking](https://arxiv.org/abs/2006.14171).
2. The [index selection evaluation platform](https://github.com/vascoG/index_selection_evaluation/tree/SWPRL) in a slightly modified version to simplify RL experiments. 
3. The [dbt5 kit](https://github.com/vascoG/dbt5) for the creation of TPC-E databases with modifiable specifications.

### Setup and example model training

```
git submodule update --init --recursive # Fetch submodules
python3.7 -m venv venv                  # Create virtualenv
source venv/bin/activate                # Activate virtualenv
pip install -r requirements.txt         # Install requirements with pip
python -m SWPRL <path_to_json_file>  # Run training example
python -m SWPRL <path_to_json_file> load  # Run application example
```

Experiments can be controlled with the (mostly self-explanatory) json-file. There is another example file in the _final_experiments_ folder. Results will be written into a configurable folder, for the test experiments it is set to _experiment\_results_. If you want to use tensoboard for logging, create the necessary folder: `mkdir tensor_log`.

The dbt5 kit allows generating and loading TPC-E benchmark data. It is recommended to populate a PostgreSQL instance via the platform with the benchmark data before executing the experiments.

For descriptions of the components and functioning, consult our dissertation. 


## Dissertation's experiments

To replicate the experiments done in the validation phase of the dissertation, follow these steps:

1. Execute ```python -m SWPRL experiments/tpce_partition.json  # or python -m SWPRL experiments/kevel_partition.json``` to train the agent.
2. Execute ```python -m SWPRL experiments/tpce_partition.json load # or python -m SWPRL experiments/kevel_partition.json load``` to get the recommended partitions.
3. You can find the recommended partitions in the ```stdout``` or in the ```final_partitions.json``` file in the corresponding experiment folder.
4. Apply those partitions manually.
5. You can analyze the performance of the queries with the new physical design with  ```SWPRL/scripts/evaluate_transactions.py``` and build comparison charts with ```SWPRL/scripts/build_charts.py```.




## JSON Configuration files
The experiments and models are configured via JSON files. For examples, check the `.json` files in the _experiments_. In the following, we explain the different configuration options:

- `id` (`str`): The name or identifier for the experiment. Output files are named accordingly.
- `description` (`str`): A textual description of the experiment. Just for documentation purposes.
- `result_path` (`str`): The path to store results, i.e., the final training report including performance numbers and the trained model files.
- `gym_version` (`int`): Version of the Partition Selection Environment Gym. Typically set to `2`. Change only if you provide an alternative Gym implementation.
- `timesteps` (`int`): The number of time steps until the training finishes. Should be chosen based on the complexity of the task. Large workloads need larger values.
- `random_seed` (`int`): The random seed for the experiment is used for everything that is based on random generators. E.g., it is passed to the StableBaselines model, TensorFlow/Pytorch, and used for workload generation.
- `parallel_environments` (`int`): The number of parallel environments used for training. Greatly impacts training durations __and__ memory usage.
- `action_manager` (`str`): The name of the action manager class to use. For more information consult the dissertation and `action_manager.py`, which contains all available managers. The dissertation's experiments use the `PartitionActionManager`.
- `observation_manager` (`str`): The name of the action manager class to use. For more information consult the dissertation and `observation_manager.py`, which contains all available managers. The dissertation's experiments use the `PartitionPlanEmbeddingObservationManager`.
- `reward_calculator` (`str`): The name of the reward calculation method to use. For more information consult the dissertation and `reward_calculator.py`, which contains all available reward calculation methods. The dissertation's experiments use the `RelativeDifferenceToPreviousReward`.
- `max_steps_per_episode` (`int`): The number of maximum admitted index selection steps per episode. This influences the time spent per training episode. The dissertation's experiments use a value of `200`.
