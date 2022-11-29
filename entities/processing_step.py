class ProcessingStep:
    total_steps = 1 # overall processing step number is tracked by the class

    def __init__(self, description: str, sub_id: int, user_id: int):
        self.description = description
        self.step_num = self.total_steps
        self.submission_id = sub_id,
        self.user_id = user_id
        ProcessingStep.increment_total_step_num()

    @classmethod
    def increment_total_step_num(cls):
        cls.total_steps += 1

    @classmethod
    def set_total_step_num(cls, new_num):
        cls.total_steps = new_num

    def __str__(self) -> str:
        return f"step_num = {self.step_num}, description = {self.description}"

    def keys(self):
        return list(self.__dict__.keys())
        
    def __getitem__(self,key):
        return getattr(self,key)