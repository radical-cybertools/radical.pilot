import asyncio
import random
from ddmd_manager import DDMD_manager

# --------------------------------------------------------------------------
#
class DDMD_dummy_workflow(DDMD_manager):
    def __init__(self, execution_backend, **kwargs):

        self._model_train_cores = kwargs.get('model_train_cores', 1)
        self._ff_train_cores = kwargs.get('ff_train_cores', 1)
        self._md_sim_cores = kwargs.get('md_sim_cores', 14)
        self._cores = self._model_train_cores + self._ff_train_cores + self._md_sim_cores

        self.uncertainties = [0] * self._md_sim_cores
        self._threshold = 5  # Threshold for uncertainty to trigger DFT task

        super().__init__(execution_backend)

        self.register_workflow_tasks()
    # --------------------------------------------------------------------------
    #
    def register_workflow_tasks(self):

        @self.flow.executable_task
        async def train_model(ind, task_description={'cpu_processes': 1}, *args):
            # Simulate a condition to close the workflow
            t_sleep = int(random.randint(0,30) / 10) + 30
            await asyncio.sleep(t_sleep)
            await self._unregister_task('task_train_model', -1)
            await self.dump(f'completed model train# {ind}, next iteration...')
            
            trigger = random.randint(1, 10)
            self._rep.plain(f'train_model trigger: {trigger}\n')
            if trigger >= 5:
                self._stop_run = True
            result  = int(random.randint(0,10) /  1)
            return f'/bin/echo  Train model task result is {result}'
        self.train_model = train_model

        # --------------------------------------------------------------------------
        #
        @self.flow.executable_task
        async def train_ff(ind, task_description={'cpu_processes': 1}, *args):
            t_sleep = int(random.randint(0,30) / 10) + 30
            await asyncio.sleep(t_sleep)
            await self._unregister_task('task_train_ff', -1)
            await self.dump(f'completed FF train# {ind}, next iteration...')
            
            # # Simulate a condition to close the workflow
            # trigger = random.randint(1, 10)
            # print(f'train_ff trigger: {trigger}')
            # if trigger >= 5:
            #     self._stop_run = True
            result  = int(random.randint(0,10) /  1)
            return f'/bin/echo  FF model task result is {result}'
        self.train_ff = train_ff

        # --------------------------------------------------------------------------
        #
        @self.flow.executable_task
        async def md_sim(ind, task_description={'cpu_processes': 1}, *args):
            t_sleep = int(random.randint(0,30) / 10) + 3
            await asyncio.sleep(t_sleep)

            await self.dump(f'completed MD sim# {ind}')


            result  = int(random.randint(0,10) /  1)
            return f'/bin/echo  MD simulation task result is {result}'
        self.md_sim = md_sim

        # --------------------------------------------------------------------------
        #
        @self.flow.executable_task
        async def md_check(ind, task_description={'cpu_processes': 1}, *args):
            # Simulated uncertainty value
            t_sleep = int(random.randint(0,30) / 10) + 3
            await asyncio.sleep(t_sleep)
            self.uncertainties[ind] = random.randint(1, 10)
            #result  = int(random.randint(0,10) /  1)

            await self.dump(f'completed MD check# {ind}')


            return f'/bin/echo  MD check task uncertainties is {self.uncertainties[ind]}'
        self.md_check = md_check

        # --------------------------------------------------------------------------
        #

        @self.flow.executable_task
        async def dft(ind, task_description={'cpu_processes': 1}, *args):
            t_sleep = int(random.randint(0,30) / 10) + 3
            await asyncio.sleep(t_sleep)

            await self._unregister_task('task_md_sim', ind)
            await self.dump(f'completed DFT# {ind}')
            

            return f"/bin/echo DFT task# {ind} completed"
        self.dft = dft

    # def get_train_tasks(self):
    #     tasks = {}

    #     n = self.workflow_loads['task_train_model']['task_num']
    #     cpus = self.workflow_loads['task_train_model']['cores_per_task']
    #     task_description={'cpu_processes': cpus}
    #     for i in range(n):
    #         tasks[f"task_train_model_{i}"] = asyncio.create_task(self.run_task(self.train_model(i, task_description)), name=f"task_train_model_{i}")

    #     n = self.workflow_loads['task_train_ff']['task_num']
    #     cpus = self.workflow_loads['task_train_ff']['cores_per_task']
    #     task_description={'cpu_processes': cpus}
    #     for i in range(n):
    #         tasks[f"task_train_ff_{i}"] = asyncio.create_task(self.run_task(self.train_ff(i, task_description)), name=f"task_train_ff_{i}")
    #     return tasks

    # def get_sim_tasks(self):
    #     tasks = {}
    #     n = self.workflow_loads['task_md_sim']['task_num']
    #     cpus = self.workflow_loads['task_md_sim']['cores_per_task']
    #     task_description={'cpu_processes': cpus}
    #     for i in range(n):
    #         tasks[f"task_md_sim_{i}"] = asyncio.create_task(self.run_sim(i, task_description), name=f"task_md_sim_{i}")
    #     return tasks
    
    async def run_sim(self, ind, task_description):
        try:
            #cpus = self.workflow_loads['task_md_sim']['cores_per_task']
            #task_description={'cpu_processes': cpus}
            while True:
                t1 = await self.md_sim(ind, task_description)
                t2 = await self.md_check(ind, task_description)

                # - if uncertainty > threshold:
                #   - ADAPTIVITY GOES HERE
                #   - run DFT task
                # - else (uncertainty <= threshold)3
                #   - MD output -> input to TASK_TRAIN_MODEL
                #   - run new MD task / run multiple MD tasks for each structure
                #     (configurable)

                if self.uncertainties[ind] > self._threshold:
                    t3 = await self.dft(ind, task_description)
                    break
        except asyncio.CancelledError:
            pass
            #self._rep.plain("md_sim failed due to cancellation during previous step\n")

        return f'completed MD Simulation# {ind}'
    
    
    def _get_all_func(self):
        
        return {'task_train_model': self.train_model,
                'task_train_ff': self.train_ff,
                'task_md_sim': self.run_sim} 

    # --------------------------------------------------------------------------
    #
    