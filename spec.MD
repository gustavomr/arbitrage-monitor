I would like to build an arbitrage monitor for tokens (pairs) into DeFi and Centralized exchanges.

The monitor should be able to:

 - Fetch prices from DeFi (DEX) and Centralized exchanges (Ex: Binance, MEXC, Bitget, Kucoin, CoinEx, Bybit)
 - Calculate the spread between the prices
 - Support config from tokens pairs and exchanges
 - Support config from minimum profit
 - Support config from minimum spread
 - Support buy or sell into CEX or DEX
 - Input an amout int token input to buy and calculate the profit as well

 - DEX: 
   - In order to buy: normally has the tax for buying
   - In order to sell: normally has the tax for selling
   - Support config from maximum slippage
   - For descentralized exchanges the buy or sell is just the oposite in the token pairs
   - Support config from tax and network transfer

 - Centralized exchanges: 
   - In order to buy: normally has the tax for buying
   - In order to transfer to another exchange or a wallet it has a network transfer (each network has your own price)
   - In order to sell: normally has the tax for selling and another tax to send to your bank account or if you like to send has a network transfer (each network has your own price)

