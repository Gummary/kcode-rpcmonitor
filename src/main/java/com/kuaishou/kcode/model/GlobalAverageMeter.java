package com.kuaishou.kcode.model;

public class GlobalAverageMeter {

    private class AverageMeter {
        private double total;
        private int n;
        private long val;
        private double average;

        private long startTimeStamp = 0;

        public AverageMeter() {
            reset();
        }

        public void reset() {
            total = 0;
            n = 0;
            val = 0;
            average = 0;

            startTimeStamp = 0L;
        }

        public void startTimer() {
            startTimeStamp = System.currentTimeMillis();
        }

        public void updateTimer() {
            long end = System.currentTimeMillis();
            Update(end - startTimeStamp);
            startTimeStamp = end;
        }

        public void Update(long val) {
            total += val;
            n += 1;
            average = total / n;
            this.val = val;
        }

        public double getTotal() {
            return total;
        }

        public int getN() {
            return n;
        }

        public long getVal() {
            return val;
        }

        public double getAverage() {
            return average;
        }

        @Override
        public String toString() {
            return String.format("Average %.3f, Total %d", average, n);
        }
    }


    private AverageMeter prepareTotalTime;
    private AverageMeter stage2Query;

    public GlobalAverageMeter() {
        prepareTotalTime = new AverageMeter();
        stage2Query = new AverageMeter();
    }

    public void resetAll() {
        prepareTotalTime.reset();
        stage2Query.reset();
    }

    public void resetPrepareTotalTime() {
        prepareTotalTime.reset();
    }
    public void resetStage2Query() {
        stage2Query.reset();
    }

    public void startPrepareTotalTime() {
        prepareTotalTime.startTimer();
    }

    public void startStage2Query() {
        stage2Query.startTimer();
    }

    public void updatePrepareTotalTime() {
        prepareTotalTime.updateTimer();
    }

    public void updateStage2Query() {
        stage2Query.updateTimer();
    }

    public void getStatistic() throws Exception {
        StringBuilder builder = new StringBuilder();
        builder.append("PrepareTotalTime\n");
        builder.append(prepareTotalTime.toString());
        builder.append("\nStage2TotalTime\n");
        builder.append(stage2Query.toString());

        throw new Exception(builder.toString());
    }

    public void getStatistic(String appendMsg) throws Exception {
        StringBuilder builder = new StringBuilder();
        builder.append("PrepareTotalTime\n");
        builder.append(prepareTotalTime.toString());
        builder.append("\nStage2TotalTime\n");
        builder.append(stage2Query.toString());
        builder.append('\n');
        builder.append(appendMsg);

        throw new Exception(builder.toString());
    }

}
