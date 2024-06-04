package shopping.cart.repository;

import java.util.Optional;
import java.util.concurrent.CompletionStage;
import shopping.cart.ItemPopularity;

public interface ItemPopularityRepository {

  CompletionStage<Long> saveOrUpdate(ItemPopularity itemPopularity);

  CompletionStage<Optional<ItemPopularity>> findById(String id);

}
