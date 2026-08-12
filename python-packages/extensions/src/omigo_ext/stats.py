from omigo_core import utils
from scipy import stats

# This is WIP
def do_ttest(xdf1, xdf2, f, n = 10000, alpha = 0.01):
    xdf1 = xdf1.select(f)
    xdf2 = xdf2.select(f)

    # find the value of n
    effective_n = min(n, xdf1.num_rows(), xdf2.num_rows())

    # perform t-test
    x_pos_1 = xdf1.sample_n(effective_n)
    x_neg_1 = xdf2.sample_n(effective_n)

    f_pos = x_pos_1.col_as_float_array(f)
    f_neg = x_neg_1.col_as_float_array(f)

    # sample may not be exact
    effective_n = min(len(f_pos), len(f_neg))
    f_pos = f_pos[0:effective_n]
    f_neg = f_neg[0:effective_n]

    # check for alpha
    if (alpha > 0):
        alpha_2_int = int(effective_n * alpha / 2)
        f_pos = f_pos[alpha_2_int:effective_n - alpha_2_int]
        f_neg = f_neg[alpha_2_int:effective_n - alpha_2_int]

    ttest = stats.ttest_ind(f_pos, f_neg, equal_var = False)
    kstest = stats.ks_2samp(f_pos, f_neg)

    mean_pos = sum(f_pos) / len(f_pos)
    mean_neg = sum(f_neg) / len(f_neg)

    utils.info("Org Len: xdf1: {}, xdf2: {}".format(xdf1.num_rows(), xdf2.num_rows()))
    utils.info("New Len: xdf1: {}, xdf2: {}".format(len(f_pos), len(f_neg)))
    utils.info("t-test: mean: xdf1: {}, xdf2: {}, pvalue: {}, statistic: {}".format(mean_pos, mean_neg, ttest.pvalue, ttest.statistic))
    utils.info("ks-test pvalue: {}, ks-test statistic: {}, location: {}, sign: {}".format(kstest.pvalue, kstest.statistic, kstest.statistic_location, kstest.statistic_sign))

