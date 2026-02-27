package padme.retention;

public interface RetentionPolicy {
  RetentionDecision onItem(long key, float[] vector);
  int storedCount();
  double totalUtility();
}
