## Running the sample code

1. Start a local DynamoDB server on default port 8000. The included `docker-compose.yml` starts everything required for running locally.

    ```shell
    docker compose up -d
    ```

2. Start a first node:

    ```shell
    mvn compile exec:exec -DAPP_CONFIG=local1.conf
    ```

3. Start `shopping-analytics-service`

4. Try it with [grpcurl](https://github.com/fullstorydev/grpcurl):

    ```shell
    # add item to cart
    grpcurl -d '{"cartId":"cart1", "itemId":"pencil", "quantity":1}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.AddItem
   
    # get cart
    grpcurl -d '{"cartId":"cart1"}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.GetCart
    
    # update quantity of item
    grpcurl -d '{"cartId":"cart1", "itemId":"pencil", "quantity":5}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.UpdateItem
    
    # check out cart
    grpcurl -d '{"cartId":"cart1"}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.Checkout
    
    # get item popularity
    grpcurl -d '{"itemId":"pencil"}' -plaintext 127.0.0.1:8101 shoppingcart.ShoppingCartService.GetItemPopularity
    ```

    Look at the log output in the terminal of the `shopping-analytics-service`.
