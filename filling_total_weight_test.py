from filling_total_weight import Pipeline

# # def complete_weight_test():

# #     pipeline = Pipeline('ingredient_quantities.csv')
# #     pack_size_split = ['5', 'bottles', 'of', '1', 'kg']
# #     item_weight = 1

# #     print(pipeline.complete_weight(pack_size_split, pack_size))


# #     def complete_weight(self, pack_size_split, item_weight, item_volume, item_density):


def test_complete_pack_size():
    pipeline = Pipeline('ingredient_quantities.csv')

    assert 5.0 == pipeline.complete_pack_size(['5', 'bottles', 'of', '1', 'kg'], 5)
    # print(pipeline.complete_pack_size([], 2))
    assert 2 == pipeline.complete_pack_size([], 2)





# # def complete_pack_size(self, pack_size_split, pack_size):
# # """
# #     This function aims to split the pack_size_text on a list of strings.
# #     This will help us to define the pack_size and the item_weight.

# #     Parameters:
# #         pack_size_split: the value of the pack_size_split column
# #         pack_size: the value of the pack_size

# #     Returns:
# #         A list containing the string splitted or an empty list
# # """
# # try:
# #     return float(pack_size_split[0]) if len(pack_size_split) > 1 else pack_size
# # except Exception as e:
# #     logging.info("Error while executing complete_pack_size: %s" % e)
# #     return []
