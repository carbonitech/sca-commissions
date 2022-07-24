class ProcessingStep:
    total_steps = 1 # overall processing step number is tracked by the class

    def __init__(self, description: str):
        self.description = description
        self.step_num = self.total_steps
        ProcessingStep.increment_total_step_num()

    @classmethod
    def increment_total_step_num(cls):
        cls.total_steps += 1

    def __str__(self) -> str:
        return f"submission_id = {self.submission_id}, step_num = {self.step_num}, description = {self.step_desctription}"

    def keys(self):
        return list(self.__dict__.keys())
        
    def __getitem__(self,key):
        return getattr(self,key)