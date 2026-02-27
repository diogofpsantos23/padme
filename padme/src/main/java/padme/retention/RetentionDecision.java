package padme.retention;

import padme.store.HeapEntry;

public final class RetentionDecision {
  public enum Kind { ADMITTED, DROPPED, EVICTED_AND_ADMITTED }

  public enum DropCause { DUPLICATE, UTILITY }

  public final Kind kind;
  public final HeapEntry admitted;
  public final HeapEntry evicted;
  public final DropCause dropCause;

  private RetentionDecision(Kind kind, HeapEntry admitted, HeapEntry evicted, DropCause dropCause) {
    this.kind = kind;
    this.admitted = admitted;
    this.evicted = evicted;
    this.dropCause = dropCause;
  }

  public static RetentionDecision admitted(HeapEntry admitted) {
    return new RetentionDecision(Kind.ADMITTED, admitted, null, null);
  }

  public static RetentionDecision dropped() {
    return droppedUtility();
  }

  public static RetentionDecision droppedUtility() {
    return new RetentionDecision(Kind.DROPPED, null, null, DropCause.UTILITY);
  }

  public static RetentionDecision droppedDuplicate() {
    return new RetentionDecision(Kind.DROPPED, null, null, DropCause.DUPLICATE);
  }

  public static RetentionDecision evictedAndAdmitted(HeapEntry evicted, HeapEntry admitted) {
    return new RetentionDecision(Kind.EVICTED_AND_ADMITTED, admitted, evicted, null);
  }
}