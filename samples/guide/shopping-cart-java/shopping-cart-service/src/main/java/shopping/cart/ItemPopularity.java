package shopping.cart;

public record ItemPopularity(String itemId, long count) {

  public ItemPopularity() {
    // null version means the entity is not on the DB
    this("", 0);
  }

  public ItemPopularity changeCount(long delta) {
    return new ItemPopularity(itemId, count + delta);
  }
}
