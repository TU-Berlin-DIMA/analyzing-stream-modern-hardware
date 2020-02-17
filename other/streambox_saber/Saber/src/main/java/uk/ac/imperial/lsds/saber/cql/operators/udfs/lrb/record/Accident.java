package uk.ac.imperial.lsds.saber.cql.operators.udfs.lrb.record;

import java.io.Serializable;

public class Accident implements Serializable {

    public int vid1, vid2, time;

    public Accident(int vid1, int vid2, int time) {
        this.vid1 = vid1;
        this.vid2 = vid2;
        this.time = time;
    }

}
