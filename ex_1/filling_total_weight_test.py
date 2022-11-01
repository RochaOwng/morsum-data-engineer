from filling_total_weight import Pipeline
import math

def test_complete_pack_size():
    pipeline = Pipeline('ingredient_quantities.csv')

    assert 5.0 == pipeline.complete_pack_size(['5', 'bottles', 'of', '1', 'kg'], 5)
    assert 2 == pipeline.complete_pack_size([], 2)


def test_complete_weight():
    pipeline = Pipeline('ingredient_quantities.csv')
   
    assert pipeline.complete_weight(['5', 'bottles', 'of', '1', 'kg'], math.nan, math.nan, math.nan) == 1
    assert pipeline.complete_weight([''], math.nan, 2, 2) == 4
    assert pipeline.complete_weight(['5', 'bottles', 'of', '1', 'kg'], 5, math.nan, math.nan) == 5
