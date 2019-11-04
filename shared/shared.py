def mtd_rtns_to_daily_log_rtns(pct_rtns: pd.DataFrame) -> pd.DataFrame:
    """Convert MTD cumulative return into daily return.

    Parameters
    ----------
    pct_rtns : pd.DataFrame
        Daily security level MTD cumulative percentage return. Assumes input
        data represent a month's MTD returns.

    Returns
    -------
    pd.DataFrame
        Daily log return.
    """
    pct_rtns = pct_rtns.sort_index(ascending=True)

    # convert to cumulative log return
    indexed = 1 + pct_rtns
    log_rtns = perf.log_returns(indexed)

    # first day won't have return, so need to fill it with original data
    first = perf.pct_to_log_return((pct_rtns).iloc[[0], :], fillna=False)
    log_rtns.update(first)

    return log_rtns


def get_excess_daily(
    data: pd.DataFrame = None, secs=None, bb_start: str = None, debug=False,
):
    if data is None:
        data = blp.getHistLast(secs, start=bb_start, field=sym.INDEX_EXCESS_MTD)
        # return data is in pct units, so convert to decimal
        data /= 100.0

    # group by year/month
    g = data.groupby([data.index.year, data.index.month])
    ylist = []
    for key, v in g:
        # for each year-month, convert MTD cum. returns to daily log returns
        # print(key, len(v))
        y = mtd_rtns_to_daily_log_rtns(v)
        ylist.append(y)

    log_daily = pd.concat(ylist, axis=0)

    # validate, reverse the process and work out monthly return from daily
    # log return and ensure on month end business days, this equals original
    # data
    mtd_check = log_daily.resample("BM").sum()
    if debug:
        print(
            f"Validation, mtd_check\n{mtd_check.head() * 100}\n\n"
            + f"original data:\n{data.head()}"
        )
    assert hp.nan_array_all_equals(
        perf.log_to_pct_return((mtd_check)) * 100,
        # ASSUMPTION: ignores NaN data points, since after resample, they
        # would be turned into 0.0
        data.resample("BM").last().fillna(0.0) * 100,
    )

    return log_daily

