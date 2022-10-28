import pandas as pd 

class Pipeline():
    def __init__(self, path):
        self.df = pd.read_csv(path)
        pass

    def complete_weight(self, pack_size_split, item_weight, item_volume,item_density):
        is_nan = item_weight != item_weight

        if not is_nan:
            return item_weight
        else:
            try:
                index_size = pack_size_split.index('kg')
                return float(pack_size_split[index_size - 1])
            except Exception as e:
                print("Error while executing pack_size_split: %s" % e)
                return item_volume * item_density

    def complete_pack_size(self, pack_size_split, pack_size):
        try:
            return float(pack_size_split[0]) if len(pack_size_split) > 1 else pack_size
        except Exception as e:
            print("Error while executing complete_pack_size: %s" % e)
            return None

    def complete_item_weight(self, item_density, item_volume, item_weight):
        try:
            return item_density * item_volume
        except Exception as e:
            print("Error while executing complete_item_weight: %s" % e)
            return item_weight

    def exec(self):
        self.df['pack_size_text'] = self.df[['pack_size_text']].fillna('')
        self.df['pack_size_split'] = self.df['pack_size_text'].apply(lambda x: x.split(' ')) 
       
        self.df['pack_size'] = self.df.apply(lambda x: self.complete_pack_size(x['pack_size_split'], x['pack_size']), axis=1)
        self.df['item_weight'] = self.df.apply(lambda x: self.complete_weight(x['pack_size_split'], x['item_weight'], x['item_volume'], x['item_density']), axis=1)
        self.df['total_weight'] = self.df['pack_size'] * self.df['item_weight']

        self.df = self.df.drop(columns=['pack_size_split'])
        return self.df


pipeline = Pipeline('ingredient_quantities.csv')
df = pipeline.exec()

print(df)