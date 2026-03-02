package padme.metrics;

import padme.retention.RetentionDecision;

public final class Metrics {
  public long seen = 0;
  public long admitted = 0;
  public long dropped = 0;
  public long evicted = 0;

  public long droppedDuplicate = 0;
  public long droppedUtility = 0;

  public long winSeen = 0;
  public long winAdmitted = 0;
  public long winDropped = 0;
  public long winEvicted = 0;

  public double winSumUSeen = 0.0;
  public double winSumUAdmitted = 0.0;
  public double winSumUDropped = 0.0;

  public long winSeenUCount = 0;
  public long winAdmittedUCount = 0;
  public long winDroppedUCount = 0;

  public long winRepReplaced = 0;

  public void record(RetentionDecision d) {
    switch (d.kind) {
      case ADMITTED -> admitted++;
      case DROPPED -> {
        dropped++;
        if (d.dropCause == RetentionDecision.DropCause.DUPLICATE) droppedDuplicate++;
        else droppedUtility++;
      }
      case EVICTED_AND_ADMITTED -> { evicted++; admitted++; }
    }
  }

  public void winRecordSeen(double u) {
    winSeen++;
    if (Double.isFinite(u)) {
      winSumUSeen += u;
      winSeenUCount++;
    }
  }

  public void winRecordAdmitted(double u) {
    winAdmitted++;
    if (Double.isFinite(u)) {
      winSumUAdmitted += u;
      winAdmittedUCount++;
    }
  }

  public void winRecordDropped(double u) {
    winDropped++;
    if (Double.isFinite(u)) {
      winSumUDropped += u;
      winDroppedUCount++;
    }
  }

  public void winRecordEvicted() {
    winEvicted++;
  }

  public void winRecordRepReplaced() {
    winRepReplaced++;
  }

  public double winAdmitRate() {
    if (winSeen == 0) return 0.0;
    return (double) winAdmitted / (double) winSeen;
  }

  public double winMeanSeenU() {
    if (winSeenUCount == 0) return 0.0;
    return winSumUSeen / (double) winSeenUCount;
  }

  public double winMeanAdmittedU() {
    if (winAdmittedUCount == 0) return 0.0;
    return winSumUAdmitted / (double) winAdmittedUCount;
  }

  public double winMeanDroppedU() {
    if (winDroppedUCount == 0) return 0.0;
    return winSumUDropped / (double) winDroppedUCount;
  }

  private void winReset() {
    winSeen = 0;
    winAdmitted = 0;
    winDropped = 0;
    winEvicted = 0;
    winSumUSeen = 0.0;
    winSumUAdmitted = 0.0;
    winSumUDropped = 0.0;
    winSeenUCount = 0;
    winAdmittedUCount = 0;
    winDroppedUCount = 0;
    winRepReplaced = 0;
  }

  public void maybePrint(int every, int storedCount, long storedBytes, double utilitySum, long elapsedNs, double uMinStore, int repsSize, double repsMinU, double repsMeanU) {
    if (every <= 0) return;
    if (seen == 0) return;
    if (seen % every != 0) return;

    double secs = elapsedNs / 1e9;
    double density = (storedBytes <= 0) ? 0.0 : (utilitySum / (double) storedBytes);

    System.out.printf(
            "t=%.3fs | seen=%d stored=%d bytes=%d admitted=%d dropped=%d(dup=%d util=%d) evicted=%d utilityDensity=%.5f | " +
                    "window(K=%d): admitRate=%.2f adm=%d drop=%d evc=%d repChange=%d | " +
                    "u(window): meanSeen=%.5f meanAdm=%.5f meanDrop=%.5f | " +
                    "uMinStore=%.5f | reps: size=%d uMin=%.5f uMean=%.5f%n",
            secs, seen, storedCount, storedBytes, admitted, dropped, droppedDuplicate, droppedUtility, evicted, density,
            every, winAdmitRate(), winAdmitted, winDropped, winEvicted, winRepReplaced,
            winMeanSeenU(), winMeanAdmittedU(), winMeanDroppedU(),
            uMinStore, repsSize, repsMinU, repsMeanU
    );

    winReset();
  }
}