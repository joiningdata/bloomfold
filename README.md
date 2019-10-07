# BloomFold

Bloom filters suitable for variable-sized set-set comparison from pre-defined sample populations. Central design principle is that the bitfield can be "folded" over to collapse sparse representations and allow for more efficient data processing.

## Background

Given a population of 12,000 items and a m=128kbit bloom filter with k=3 hash functions, one would expect a 1.4% false positive rate.
Given a sample of 491 items from the same population, only 5,363 bits would be necessary to hold the estimated error rate around 1.4%.
Rounding to the nearest power of two for implementation's sake gives 8192 bits and approximately 0.45% error estimate.