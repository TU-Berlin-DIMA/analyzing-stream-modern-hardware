package uk.ac.imperial.lsds.saber.hardware.papi;

import uk.ac.imperial.lsds.saber.hardware.AbstractHardwareSampler;
import papi.EventSet;
import papi.Papi;
import papi.Constants;
import papi.PapiException;

import java.io.Serializable;
import java.util.function.Supplier;

public class PAPIHardwareSampler implements AbstractHardwareSampler {

    private boolean start_sampling_flag;
    private transient EventSet eventSet;

    private int[] countersId;

    private String[] countersDescription;

    private String hwPerfCountersCfg;


    public PAPIHardwareSampler(String hwPerfCounters) {
        this.hwPerfCountersCfg = hwPerfCounters;
        this.start_sampling_flag = false;
    }


    @Override
    public void startSampling() throws Exception {
        System.out.println("[DBG] PID: " +  Thread.currentThread().getId() + " start sampling...");
        Papi.initThread();

        String[] tokens = hwPerfCountersCfg.split(",");

        Constants placeholder = new Constants();

        countersId = new int[tokens.length];
        countersDescription = new String[tokens.length];

        for (int i = 0; i < tokens.length; i++) {

            String token = tokens[i].trim();
            String tokenName = token + "_name";

            try {
                countersId[i] =
                        Constants.class.getDeclaredField(token).getInt(placeholder);

                countersDescription[i] = (String)
                        Constants.class.getDeclaredField(tokenName).get(placeholder);

                // LOG.info("GREPTHIS :: Event {} with id 0x{} added", countersDescription[i], Integer.toHexString(countersId[i]));
                System.out.println("GREPTHIS :: PID " + Thread.currentThread().getId() + " Event " + countersDescription[i] + " with id 0x" + Integer.toHexString(countersId[i]) + " added.");

            } catch (NoSuchFieldException ex) {
                // LOG.info("GREPTHIS :: {} is not available and will be ignored", token);
                System.out.println("GREPTHIS :: {} is not available and will be ignored " + token);
            }

        }

        eventSet = EventSet.create(countersId);

        eventSet.start();

        this.start_sampling_flag = true;
    }

    @Override
    public void stopSampling(String prefix) throws Exception {
        if (! this.start_sampling_flag) {
            System.out.println("[ERROR]: startSampling must be called before stopSampling");
            System.exit(-1);
        }
        if (eventSet == null) {
            System.out.println("event set is NULL");
            return;
        }

        eventSet.stop();

        long[] counters = eventSet.getCounters();

        for (int i = 0; i < counters.length; i++) {
            System.out.println("GREPTHIS :: " + prefix + " PID: " + Thread.currentThread().getId() + " :: counter[" +
                               countersDescription[i] + "] = [" + counters[i] + "]");
            // LOG.info("GREPTHIS :: {} :: counter [{}]=[{}]",
            //         prefix, countersDescription[i], counters[i]);
        }

        eventSet.destroy();
    }


}
