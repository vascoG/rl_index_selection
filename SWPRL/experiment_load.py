import copy
import importlib
import logging
import pickle
import sys

import gym_db  # noqa: F401
from gym_db.common import EnvironmentType

from .experiment import Experiment

def load_and_run():
    logging.basicConfig(level=logging.INFO)

    assert len(sys.argv) == 2, "Experiment configuration file must be provided: main.py path_fo_file.json"
    CONFIGURATION_FILE = sys.argv[1]

    experiment = Experiment(CONFIGURATION_FILE, True)

    if experiment.config["rl_algorithm"]["stable_baselines_version"] == 2:
        from stable_baselines.common.callbacks import EvalCallbackWithTBRunningAverage
        from stable_baselines.common.vec_env import DummyVecEnv, SubprocVecEnv, VecNormalize

        algorithm_class = getattr(
            importlib.import_module("stable_baselines"), experiment.config["rl_algorithm"]["algorithm"]
        )
    elif experiment.config["rl_algorithm"]["stable_baselines_version"] == 3:
        from stable_baselines3.common.callbacks import EvalCallbackWithTBRunningAverage
        from stable_baselines3.common.vec_env import DummyVecEnv, SubprocVecEnv, VecNormalize

        algorithm_class = getattr(
            importlib.import_module("stable_baselines3"), experiment.config["rl_algorithm"]["algorithm"]
        )
    else:
        raise ValueError

    experiment.prepare()

    ParallelEnv = SubprocVecEnv if experiment.config["parallel_environments"] > 1 else DummyVecEnv

    training_env = ParallelEnv(
        [experiment.make_env(env_id) for env_id in range(experiment.config["parallel_environments"])]
    )
    training_env = VecNormalize(
        training_env, norm_obs=True, norm_reward=True, gamma=experiment.config["rl_algorithm"]["gamma"], training=True
    )

    experiment.model_type = algorithm_class

    with open(f"{experiment.experiment_folder_path}/experiment_object.pickle", "wb") as handle:
        pickle.dump(experiment, handle, protocol=pickle.HIGHEST_PROTOCOL)

    
    model = experiment.load_model(algorithm_class, training_env)

    experiment.set_model(model)
   
    experiment.suggest_indexes(model)
    

if __name__ == '__main__':
    load_and_run()


