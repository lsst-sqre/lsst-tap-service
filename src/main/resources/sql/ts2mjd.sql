/*
    ts2mjd - convert a timestamp to a modified Julian date.

    A Julian Date (JD) is the days and fractions since noon Universal Time on January 1, 4713 BC (on the Julian calendar).
    A Modified Julian Date (MJD) is a Julian Date, minus 2,400,000.5 days. The extra half day is because a MJD
    by definition starts at 12:00 a.m. (midnight), while a JD starts at 12:00 p.m (noon).

    This functions adds an extra half day to the value subtracted from a JD. It subtracts 2,400,001
    instead of 2,400,000.5. The reason is the Oracle to_char Julian date conversion returns a JD that starts
    at 12:00 a.m. instead of the expected 12:00 p.m. The returned JD is a half day longer than expected, and
    the extra half day

*/
CREATE OR REPLACE FUNCTION ts2mjd(ts IN TIMESTAMP)
    RETURN NUMBER
    IS mjd NUMBER(12,6);
    jd NUMBER(8,0);
    h NUMBER(2,0);
    m NUMBER(2,0);
    s NUMBER(9,6);
BEGIN
    jd := TO_NUMBER(TO_CHAR(ts, 'J'));
    h := EXTRACT(hour FROM ts);
    m := EXTRACT(minute FROM ts);
    s := EXTRACT(second FROM ts);
    mjd := jd + (h/24) + (m/(24*60)) + (s/(24*60*60)) - 2400001;
    RETURN mjd;
END ts2mjd;
/
