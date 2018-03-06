
import pandas as pd

class GlossaryBuilder:
    def __init__(self):
        self.glossary = pd.DataFrame(columns=['variable_name',
                                                'description'])

    def describe_variable(self, variable_name, variable_description):
        self.glossary.loc[len(self.glossary)] = [variable_name, variable_description]
