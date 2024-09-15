# eex_collector

This repository contains regularly updated data from [EEX](https://www.eex.com/en/market-data/natural-gas/spot) 
Specifically, gas prices Title Transfer Facility (TTF) for End of Day (EOD) and European Gas Spot Index (EGSI) are collected.

In order to assure that the data was updated, and for redundancy, each file contains data for the last 20 days. 

The data will be used for personal project related to the energy market analysis and forecasting. 

The data is obtained via an API call. 

_NOTE_: the prices are multiplied by 9.7694 conversion factor
(conversion factors for such commodities: 9.7694 TWh/bcm for fossil gas and 8.141 TWh/)
[(source)](https://beyondfossilfuels.org/wp-content/uploads/2023/03/BFF-FreedomFromFossilFuels-2023-LowRes-2.pdf)

The code is inspired by this [repo](https://github.com/gruijter/com.gruijter.powerhour) and was translated from 
java script to pyton with the help of ChatGPT-o1-preview 
