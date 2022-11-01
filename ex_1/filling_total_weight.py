import pandas as pd 
import logging 

class Pipeline():
    def __init__(self, path):
        self.df = pd.read_csv(path)
        

    def complete_weight(self, pack_size_split, item_weight, item_volume, item_density):
        """
        This function will handle the missing weight data.
        We will apply this function to every row on the dataset, with that, the function works on a row-based level.
        
        So, we are passing as arguments only the columns we will need.

        Firstly, the function will check if item_weight is a NaN, if isn't, then we don't need to complete it. 
        Then, we will take the value referred to as the weight. 
            Note: if the text on the pack_size_text is a text but doesn't contain the 'number kg' pattern,
            this function will not work as expected.
        For last, if the previous conditions aren't true, then the function will return item_volume * item_density

            Parameters:
                pack_size_split: the value of the pack_size_split column
                item_weight: the value of the item_weight column
                item_density: the value of the item_density column

            Returns:
                A float value filling the gaps in the item_weight column
        """

        is_nan = item_weight != item_weight

        if not is_nan:
            return item_weight
        else:
            try:
                index_size = pack_size_split.index('kg')
                return float(pack_size_split[index_size - 1])
            except Exception as e:
                logging.info("Error while executing pack_size_split: %s" % e)
                return item_volume * item_density


    def complete_pack_size(self, pack_size_split, pack_size):
        """
            This function aims to split the pack_size_text on a list of strings.
            This will help us to define the pack_size and the item_weight.

            Parameters:
                pack_size_split: the value of the pack_size_split column
                pack_size: the value of the pack_size

            Returns:
                A list containing the string splitted or an empty list
        """
        try:
            return float(pack_size_split[0]) if len(pack_size_split) > 1 else pack_size
        except Exception as e:
            logging.info("Error while executing complete_pack_size: %s" % e)
            return []



    def exec(self):
        """
            This function is the core of this pipeline execution.
            Will consolidate the logic and function's calls.

            Returns:
                The Dataframe object containing the total_weight filled.
        """
        self.df['pack_size_text'] = self.df[['pack_size_text']].fillna('')
        self.df['pack_size_split'] = self.df['pack_size_text'].apply(lambda x: x.split(' ')) 
       
        self.df['pack_size'] = self.df.apply(lambda x: self.complete_pack_size(x['pack_size_split'], x['pack_size']), axis=1)
        self.df['item_weight'] = self.df.apply(lambda x: self.complete_weight(x['pack_size_split'], x['item_weight'], x['item_volume'], x['item_density']), axis=1)
        self.df['total_weight'] = self.df['pack_size'] * self.df['item_weight']

        self.df = self.df.drop(columns=['pack_size_split'])
        return self.df


pipeline = Pipeline('ingredient_quantities.csv')
# df = pipeline.exec()
df = pipeline.complete_weight(['5', 'bottles', 'of', '1', 'kg'], 1, 'NaN', 'NaN')

print(df)