# Market-Making Trading Engine
Research Findings, Live Results & Next Steps - 18 September 2026

## 1. Executive Summary
#### Strategy Status
The market-making engine has progressed from historical research and deterministic replay through paper/testnet execution to live capital deployment on Binance spot.

The current live deployment is focused on PEPEUSDT and is intentionally small. The current live run used approximately $100 of initial cash, a $10 target notional, and a maximum inventory of 2.5 units.

The current live run produced:
```
Net P&L: -$0.04245

Fees paid: $0.04065

Notional traded: $40.65

Reported Sharpe: -0.0209
```
Initial live sample: too small to draw conclusions about persistent profitability

The live result should therefore be treated primarily as an execution-validation experiment, rather than evidence for or against long-term strategy profitability.

The main research finding so far is that the market contains measurable microstructure information, but the economic value of that information depends heavily on regime, toxicity, queue position, latency, fees and inventory.

The research has therefore shifted from:
```
Can I predict short-term price movement?
```

toward:
```
Can I selectively provide liquidity when the expected value of the fill exceeds adverse selection, execution and inventory costs?
```
### 1.1. Objective
I built a production-oriented market-making engine for systematic liquidity provision under realistic market-microstructure constraints.

The system combines:
- Live L2 market data
- Deterministic historical replay
- Market discovery
- Regime classification
- Microstructure alpha
- Structural fair-value estimation
- Residual alpha
- Toxicity modelling
- Queue-aware execution simulation
- Inventory-aware quoting
- Exchange execution and reconciliation
- Live execution diagnostics

The architecture deliberately separates signal generation from execution.

The objective is not simply to predict the next price movement. It is to determine:
- where market-making conditions are attractive
- when existing signals are reliable
- whether a prospective fill is likely to be favorable or toxic
- how quotes should be priced and sized
- whether the order is likely to execute
- whether the expected edge survives fees, adverse selection and inventory risk

The central research hypothesis is therefore:
```
Selective participation under realistic execution constraints is more important than maximizing predictive accuracy.
```

### 1.2. Key Research Findings
#### H1 - Market selection can identify materially different market-making opportunities
The market-discovery layer evaluates:
- gross spread
- transaction fees
- bid/ask liquidity
- liquidity balance
- trade activity
- order-book depth

The current snapshot produced several markets with attractive displayed net spreads.

| Market    | Maker/Taker Fees | Net Spread	 | Bid Liquidity | Ask Liquidity |
|-----------|------------------|-------------|---------------|---------------|
| PARTIUSDT | 0.02%            | 0.3802%     |	$7.6k        | $1.0k         |
| ACHUSDT	  | 0.02%            | 0.3529%     | $4.2k         | $3.2k         |
| CYBERUSDT | 0.02%            | 0.2725%     | $4.2k         | $3.0k         |
| KSMUSDT	  | 0.02%            | 0.2433%     | $4.3k         | $1.2k         |
| PEPEUSDT  | 0.02%            | 0.2260%     | $206k         | $34k          |
| SKLUSDT	  | 0.02%            | 0.2211%     | $3.5k         | $1.5k         |
| GMXUSDT	  | 0.02%            | 0.2263%     | $1.1k         | $1.3k         |
| SHIBUSDT  | 0.02%            | 0.1494%     | $11k          |	$21k         |
| APTUSDT	  | 0.02%            | 0.1379%     | $11.2k        | ~$7–10k       |

PEPEUSDT currently provides the strongest combination of displayed spread and absolute depth among the observed candidates.

However, the book is materially asymmetric, with approximately $206k of bid liquidity versus $34k of ask liquidity.

This highlighted an important distinction:
```
Displayed spread is not sufficient evidence of a good market-making opportunity.
```
The opportunity must also survive liquidity imbalance, fill probability, adverse selection, queue dynamics and inventory risk.

PEPEUSDT is therefore currently being used as the primary live research market.

#### H2 - Market regimes materially change the trading environment
I implemented an unsupervised Gaussian Mixture Model using:
- spread
- volatility
- order imbalance
- trade imbalance
- microprice deviation

The current three-regime classification is:

| Regime	           | Characteristics                                 | 
|--------------------|-------------------------------------------------|
| 0:	Low vol        |  Lower volatility, relatively stable conditions |
| 1:	High vol       |  High volatility                                |
| 2:	Directional    | Large directional imbalance                     |

The current regime statistics show materially different future behaviour across regimes.

The directional regime, for example, has substantially more negative future returns and higher future volatility than the low-volatility regime.

The regime model is therefore currently being used primarily as a conditional gate, rather than as an independent alpha source.

The objective is not to predict returns directly from the regime.

Instead:
```
The regime determines when existing signals and liquidity-provision assumptions should be trusted.
```
This affects:
- whether to quote
- quote width
- quote skew
- size
- inventory tolerance
- reliance on microstructure alpha
- tolerance for adverse selection

The current classifier is useful as a first segmentation of the market, but requires recalibration before being treated as a stable production component.

#### H3 - Microprice contains predictive information beyond the immediate quote horizon
I tested the microprice signal:
```
micro_signal_t = microprice_t − mid_t
```
against future mid-price returns without incorporating the structural fair-value adjustment.

The current out-of-sample diagnostics are:

| Horizon	IC	| Rank IC	| Residual Rank IC | Hit Rate	| PnL Proxy |	Sharpe Proxy  |
|-------------|---------|------------------|----------|-----------|---------------|
| 100ms	      | 0.129	  | 0.090	           | 0.003	  | 9.31e-9 	| 0.055         |
| 500ms	     	| 0.200  	| 0.145            | 0.009	  | 2.35e-8	  | 0.089         |
| 1s		      | 0.273	  | 0.200          	 | 0.016   	| 4.35e-8	  | 0.121         |
| 5s		      | 0.416   | 0.330            | 0.049   	| 1.16e-7 	| 0.207         |

The strongest observation is that the relationship increases with horizon rather than disappearing immediately.

The microprice signal therefore appears more useful as a short-horizon fair-value correction than as an ultra-low-latency prediction of the next trade.

This is important for the architecture.

Rather than using microprice alpha to aggressively predict the next tick, the current evidence supports testing whether it can improve:
- reservation price
- quote skew
- side selection
- inventory adjustment
- short-horizon fair-value estimation

However, the IC results alone do not establish profitability. The next test is whether the signal improves realized market-making P&L after fees, fills and adverse selection.

#### H4 - Live execution is exposing the gap between simulated and real market making
The live system currently uses the regime classifier without the microprice-alpha model, residual model or toxicity model.

The live stack therefore provides a relatively clean baseline for evaluating the effect of progressively adding these components.

Current configuration:
```
PEPEUSDT spot

$100 initial cash

$1.1 base size

$2.5 maximum inventory

0.1 inventory-risk parameter

0.02% maker fee

0.02% taker fee
```

The initial live run produced:
```
P&L: -$0.04245

Fees: $0.04065

Notional traded: $40.65
```

The loss is small in absolute terms, but the sample is far too small to infer anything about the long-term profitability of the strategy.

More importantly, the live run provides information about:
- actual fills
- maker/taker execution
- realized markouts
- quote behaviour
- inventory
- exchange event handling
- differences between simulated and live execution

The purpose of this stage is therefore:
```
Validate the assumptions required for the strategy to work before increasing complexity or capital.
```

#### H5 - Live fills show significant adverse selection in the current baseline
The current live fill dataset produces the following average signed markouts:
| Horizon | Buy Markout	| Sell Markout |
|---------|-------------|--------------|
| 100ms   | -14.07 bps  | -12.74 bps   |
| 500ms   | -14.07 bps  | -12.74 bps   |
| 1s      | -14.07 bps  | -12.74 bps   |
| 5s      | -2.47 bps   | -7.38 bps    |

The most important observation is that the current live fills show negative signed markouts at the measured horizons.

This suggests that the baseline execution policy is receiving fills that are followed by adverse price movement.

That is precisely the problem the toxicity model is intended to address.

However, the current sample is too small to determine whether these markouts represent a persistent property of the strategy or simply a small-sample effect.

There is also evidence that the current markout instrumentation requires further validation, since the 100ms, 500ms and 1s values are identical in the current output.

Before drawing stronger conclusions, I therefore need to verify:
- timestamp alignment;
- snapshot-to-fill matching;
- future-mid lookup;
- exchange event timestamps;
- fill timestamps;
- duplicate observations;
- missing future observations.

The current result is therefore best interpreted as an execution diagnostic, not yet a statistically established toxicity estimate.

#### H6 - The initial toxicity model cannot yet be evaluated reliably
I trained an XGBoost model to predict signed future markouts conditional on execution.

The intended target is:
```
T(x) = E[future signed markout_h ∣ fill, state]
```
with inputs including:
- spread
- volatility
- order imbalance
- trade imbalance
- microprice deviation
- regime

The model is intended to distinguish:
```
If I get filled here, was I providing liquidity to informed or adverse flow?
```
The current test output has NaN IC values and extremely small/constant prediction statistics.

This is consistent with the current live dataset being too small or insufficiently variable for meaningful model evaluation.

The current toxicity model should therefore not yet be treated as validated.

The next priority is to accumulate a sufficiently large and diverse live-fill dataset, verify the markout labels, and establish a proper train/test methodology before using toxicity predictions to gate live trading.

#### 1.3. What I Learned
The main lesson from the project is:
```
Market making is not primarily a forecasting problem. It is a conditional execution problem.
```
The research initially focused on whether microstructure variables could predict future price movement.

Live deployment changed the framing.

A predictive signal only has economic value if:
- the signal is present when the quote is generated
- the quote arrives while the information is still relevant
- the order joins a sufficiently favorable queue
- the order actually fills
- the fill does not occur disproportionately during adverse information events
- the captured spread survives fees and slippage
- repeated fills do not create unacceptable inventory risk

This led me to separate the trading edge into distinct components:
```
Alpha
    → where edge exists

Regime
    → when the edge is reliable

Toxicity
    → whether providing liquidity is attractive

Reservation Price
    → how the edge is incorporated into quotes

Queue
    → probability of actually getting filled

Latency
    → whether the quote is still relevant when it arrives

Execution
    → whether theoretical edge becomes realized edge

Fees
    → whether realized edge survives costs

Inventory
    → whether repeated fills create unacceptable directional risk
```

This decomposition makes the strategy substantially more falsifiable.

A model can have a strong IC while producing little or no trading value if:
- fills occur too late
- the queue is unfavorable
- adverse selection dominates spread capture
- fees consume the edge
- inventory management forces taker execution

The live deployment has therefore reinforced the importance of execution-conditioned research rather than unconditional alpha prediction.

#### 1.4. Current Research Priorities
The immediate research priorities are:
1. Recalibrate and validate the regime classifier.
2. Validate and potentially integrate microprice alpha.
3. Build a statistically meaningful toxicity dataset.
4. Train and validate the toxicity model on live fills.
5. Determine whether toxicity predictions improve quote selection and sizing.
6. Add residual alpha only if the existing signal stack leaves measurable unexplained predictive information.
7. Continue small-capital live validation before increasing risk.

The research progression is therefore:
```
Market Discovery
       ↓
Regime Classification
       ↓
Microstructure Alpha
       ↓
Toxicity
       ↓
Quote / Skew / Size
       ↓
Queue + Latency
       ↓
Execution
       ↓
Inventory / Risk
       ↓
Realized P&L
```

## 2. Research Objectives and Hypotheses
I separated the market-making system into independent hypotheses so that each component can be tested without attributing aggregate P&L to a single model.
```
                    Market-Making Edge
                           │
             ┌─────────────┴─────────────┐
             │                           │
             ▼                           ▼
       Signal Quality              Execution Quality
             │                           │
       ┌─────┼─────┐             ┌───────┼───────┐
       │     │     │             │       │       │
      H1    H2    H3            H4      H5      H6
       │     │     │             │       │       │
    Regime Micro  Residual     Toxicity Queue  Latency
            Alpha   Alpha
       │     │     │             │       │       │
       └─────┴─────┴─────────────┴───────┴───────┘
                           ↓
                    Quoting Policy
                           ↓
                       Execution
                           ↓
                          Risk
                           ↓
                     Realized P&L
```
                     
| Hypothesis | Component        | Question	                                                                       | Primary Risk                            |
|------------|------------------|----------------------------------------------------------------------------------|-----------------------------------------|
| H1	       | Regime           | Does regime classification identify materially different execution environments? | Non-stationarity / misclassification    |
| H2	       | Microprice alpha | Does microprice deviation contain useful short-horizon information?              | Signal decay / execution lag            |
| H3	       | Residual alpha   | Does ML capture information not explained by structured fair value?              | Overfitting                             |
| H4	       | Toxicity         | Can conditional markouts identify adverse-selection risk before quoting?         | Selection bias / insufficient fills     |
| H5	       | Queue	          | Can queue dynamics predict passive fill probability?                             | Incorrect cancellation/depletion model  |
| H6	       | Latency	        | Does execution latency materially change realized edge?                          | Stale quotes / adverse selection        |

This separation allows each component to be evaluated independently rather than relying on aggregate live P&L.

## 3. Market Discovery
The market-discovery layer evaluates whether a market provides sufficient conditions for systematic liquidity provision.

The core objective is:
```
Net Spread = Gross Spread − Fees − Expected Execution Costs
```

Liquidity is evaluated conservatively using the smaller side of the order book:
```
Lmin = min(BidLiquidity, AskLiquidity)
```

and liquidity balance:
```
Balance = min(Bid,Ask) / max(Bid,Ask)
```
This prevents markets with a large displayed spread but highly asymmetric or shallow liquidity from automatically being treated as attractive.

The current snapshot identified PEPEUSDT as the main deployment market because of its combination of:
```
approximately 0.226% displayed net spread;

substantially larger absolute displayed liquidity than most alternatives;

sufficient market activity for continued execution research.
```

The major caveat is its substantial bid/ask liquidity asymmetry.

This provides a useful live test of whether displayed liquidity actually translates into executable market-making economics.

## 4. Regime Classification
The regime classifier uses a Gaussian Mixture Model over:
- spread
- volatility
- order imbalance
- trade imbalance
- microprice deviation

The current regimes are interpreted as:

#### Regime 0 - Low Volatility
Lower volatility and relatively stable market conditions.

Observed future return is close to zero and future volatility is comparatively low.

#### Regime 1 - High Volatility
Higher volatility with more active price formation.

The regime exhibits higher future volatility and therefore potentially greater adverse-selection and inventory risk.

#### Regime 2 - Directional
Large order-book imbalance and microprice displacement.

This regime has materially negative observed future return and higher future volatility in the current sample.

The classifier is currently being used as a conditional control variable rather than direct alpha.

The next research step is to determine whether the regime boundaries remain stable across:
- different time periods
- volatility environments
- market conditions
- different instruments

If the clustering is unstable, I will recalibrate the model rather than treating the current labels as permanent market states.

## 5. Microprice Alpha
The microprice signal is:
```
micro_signal = microprice − mid
```

I tested the signal against future mid-price returns without incorporating the structural fair-value model.

The results show increasing predictive information across the tested horizons.

The strongest result is at 5 seconds:
```
IC: 0.416

Rank IC: 0.330

Sharpe proxy: 0.207
```
The evidence suggests that the microprice signal contains information about subsequent price evolution.

However, this does not yet establish that the signal produces positive market-making returns.

The key question is now:
```
Does incorporating microprice information into reservation prices and quote skew improve realized P&L after fees and adverse selection?
```
The next implementation will therefore be controlled.

Rather than allowing the model to directly determine trading direction, I will test microprice alpha as a bounded adjustment to:
- fair value
- reservation price
- quote skew
- order size

This preserves the distinction between prediction and execution policy.

6. Live Execution Results

Current Live Run
```
Live Since:         2026-09-03
Market:        	    PEPEUSDT
Venue:	            Binance Spot
Initial Cash:	      $100
Base Size:          $1.1
Maximum Inventory:  $2.5
Net P&L:            -$0.04245
Fees Paid:          $0.04065
P&L per fill:       -42449899.40 PEPE
Fees per fill:      40653199.40 PEPE
Notional Traded:    $40.65
Reported Sharpe:    -0.0209
```

The live sample is too small to estimate persistent profitability.

The negative P&L should therefore not be interpreted as evidence that the complete strategy has negative expected return.

At the same time, it should not be ignored.

The current result provides evidence that the baseline execution configuration has not yet demonstrated that it can reliably convert displayed spread into realized economic value.

The most useful output from this run is therefore the execution dataset.

## 7. Live Markout Analysis
The live system records each fill together with:
- side
- price
- quantity
- maker/taker status
- signal state
- regime
- spread
- volatility
- order imbalance
- trade imbalance
- microprice deviation
- snapshot age
- execution latency
- time to fill
- future signed markouts

This creates the following research chain:
```
Market State
     ↓
Quote Decision
     ↓
Order Placement
     ↓
Queue / Latency
     ↓
Fill
     ↓
Future Price Path
     ↓
Signed Markout
```
This is more informative than evaluating fills only through aggregate P&L.

The current live sample shows negative markouts for both buy and sell fills, particularly at short horizons.

This is consistent with the hypothesis that the strategy is currently exposed to adverse selection.

However, the current dataset is not yet large enough for statistical inference.

The immediate task is therefore to improve the measurement infrastructure before optimizing the model around it.

## 8. Toxicity Modelling
The toxicity model is intended to predict:
```
E[ Markout_h ∣ Fill , State]
```
The important distinction is that the target is conditional on execution.

The model is not simply asking:
```
Where will price move?
```
It is asking:
```
If my passive order gets filled in this state, what is the expected subsequent markout?
```
This makes toxicity directly relevant to liquidity provision.

The proposed model uses:
- spread
- volatility
- order imbalance
- trade imbalance
- microprice deviation
- regime

The intended outputs are:
```
Predicted favorable markout
          ↓
Increase participation

Neutral
          ↓
Normal quoting

Predicted adverse markout
          ↓
Widen / skew / reduce size / stop quoting
```

The current toxicity model cannot yet be considered validated.

The live dataset is too small, and the current test produces NaN correlation statistics and effectively constant predictions.

The next step is therefore not to optimize the XGBoost model.

It is to build a better dataset.

The research sequence will be:
```
Live Fills
    ↓
Validated Markout Labels
    ↓
Train / Test Split
    ↓
Toxicity Model
    ↓
Out-of-Sample Predictions
    ↓
Conditional Fill Analysis
    ↓
Execution Policy
```

## 9. What I Learned
The project has changed my view of market making from a signal-generation problem into an execution problem.

The most important lesson is:
```
The value of an alpha signal is conditional on whether it can be monetized through the execution mechanism.
```

A signal can have predictive information but still fail economically because:
- the information decays before order arrival;
- the order joins an unfavorable queue;
- fills occur selectively during adverse events;
- spread capture is insufficient after fees;
- inventory management creates directional exposure;
- cancellation and replacement decisions introduce execution costs.

This is why I now evaluate the strategy as a chain of conditional decisions rather than as a single predictive model.

The current architecture therefore separates:
Where should I trade?

Market discovery.

When should I provide liquidity?

Regime classification.

Where should fair value be?

Structural and microprice alpha.

Is a fill likely to be favorable?

Toxicity.

Will I actually get the fill?

Queue and execution modelling.

Will the strategy survive repeated fills?

Inventory and risk management.

This decomposition also makes failure easier to diagnose.

If the strategy loses money, I can ask whether the problem came from:

incorrect fair value;

incorrect regime classification;

adverse selection;

queue estimation;

latency;

fees;

inventory;

or execution logic.

That is more useful than simply optimizing aggregate P&L.

## 10. Next Steps
#### 10.1. Recalibrate the Regime Classifier
The current GMM provides a useful first segmentation, but the regime boundaries need to be tested for stability.

I will:
- recompute regime distributions over additional historical periods
- evaluate cluster stability
- examine transition probabilities
- test regime-specific alpha decay
- compare future volatility and markouts across regimes
- determine whether regime-specific quoting policies improve execution

The goal is to establish whether the regime classifier provides stable conditional information rather than simply fitting the current snapshot distribution.

#### 10.2. Add Microprice Alpha to the Quoting Policy
The current microprice results justify testing the signal as an additional fair-value component.

The initial implementation will be conservative:
```
Structural Fair Value
        +
Microprice Alpha
        ↓
Reservation Price
        ↓
Inventory Adjustment
        ↓
Quote Skew
```

I will compare:

Baseline
```
Regime → Structural Fair Value → Quote
```
against:

Enhanced
```
Regime → Structural Fair Value + Microprice Alpha → Quote
```
The primary evaluation will be realized execution economics rather than IC alone.

Metrics will include:
- realized spread
- signed markout
- fill rate
- adverse-selection cost
- inventory
- fees
- P&L per unit of notional
- P&L per fill

#### 10.3. Build the Toxicity Dataset
The immediate priority is to collect more live fills with correctly aligned markout labels.

Each observation should contain:
- state at quote
- state at order entry
- state at fill
- time to fill
- queue estimate
- latency
- maker/taker status
- future mid prices
- future microprice
- signed markouts
- inventory
- regime
- quote distance

Before model training, I will validate the label-generation pipeline to ensure that the 100ms, 500ms, 1s and 5s horizons represent genuinely different future observations.

#### 10.4. Validate Toxicity Out of Sample
Once sufficient live data exists, I will evaluate whether predicted toxicity separates fills into different realized markout distributions.

The key test is not simply model accuracy.

It is:
```
Do fills predicted to be toxic actually have worse realized economics than fills predicted to be favorable?
```
I will therefore evaluate:
- IC;
- Rank IC;
- calibration;
- markout by toxicity bucket;
- fill rate by bucket;
- realized spread by bucket;
- post-fee P&L by bucket.

The final test is whether using the toxicity signal in the execution policy improves realized market-making economics.

#### 10.5. Evaluate Residual Alpha Only If Necessary
Residual alpha will remain a secondary component.

The current microprice and structural models should first be tested together.

Only if meaningful unexplained residual information remains will I add the XGBoost residual model.

The purpose is to avoid adding model complexity without measurable incremental value.

The comparison will therefore be:
```
Structural Alpha
      ↓
Structural + Microprice
      ↓
Structural + Microprice + Residual
```

Each additional component must demonstrate incremental out-of-sample value after realistic execution costs.

#### 10.6. Improve Queue and Latency Modelling
Live deployment provides data that cannot be fully reproduced through historical snapshots.

I will use live execution to estimate:

order-placement latency;

exchange acknowledgement delay;

cancellation latency;

queue position;

trade-driven depletion;

cancellation-driven depletion;

quote churn;

snapshot staleness;

time-to-fill distributions.

The goal is to close the gap between:
```
Historical Simulation
        ↓
Expected Fill
```

and:
```
Real Exchange
        ↓
Actual Fill
```

This is particularly important because a market-making strategy can have positive theoretical edge while producing negative realized returns if its queue and latency assumptions are too optimistic.

## 11. Falsification Criteria
The purpose of the research process is to allow the strategy to fail.

#### 11.1. Regime Model
The regime hypothesis will be weakened if:
- cluster definitions are unstable across periods;
- regimes do not produce materially different execution characteristics;
- regime-conditioned policies do not improve out-of-sample economics;
- results depend heavily on arbitrary feature scaling or initialization.

#### 11.2. Microprice Alpha
The microprice hypothesis will be weakened if:
- predictive information disappears out of sample;
- the signal does not improve fair-value estimation;
- improvements in IC do not translate into execution economics;
- transaction costs and adverse selection consume the signal;
- performance is highly sensitive to a small number of periods.

#### 11.3. Toxicity Model
The toxicity hypothesis will be weakened if:
- predicted toxicity does not separate realized markouts;
- model performance disappears out of sample;
- filtering toxic fills reduces participation more than it improves economics;
- the model does not improve realized P&L after accounting for missed fills.

#### 11.4. Overall Strategy
The market-making strategy will be weakened if:
- displayed spreads consistently fail to translate into realized spread capture;
- adverse selection dominates spread capture;
- queue and latency costs consume the available edge;
- inventory management materially reduces expected returns;
- performance disappears after realistic fees and execution costs.

## 12. Research Framework Going Forward
The current research framework is:
```
              Hypothesis
                   ↓
              Measurement
                   ↓
          Out-of-Sample Test
                   ↓
             Execution Test
                   ↓
                 Risk
                   ↓
            Live Validation
                   ↓
             Realized P&L
```
The important change is that each layer must survive independently:
- A strong predictive signal is not sufficient.
- A favorable backtest is not sufficient.
- A wide displayed spread is not sufficient.
- A high fill rate is not sufficient.

The strategy ultimately needs to demonstrate that:
```
Expected Liquidity Value > Adverse Selection + Fees + Execution Costs + Inventory Risk
```
under realistic live conditions.

## 13. Conclusion
The market-making engine has progressed from historical simulation to live execution and now provides a framework for testing the full relationship between market microstructure, predictive signals and executable liquidity provision.

The current research has produced several useful findings:

Market discovery identifies materially different liquidity and spread environments, with PEPEUSDT currently providing the deepest observed market for live deployment.

The regime classifier identifies distinct low-volatility, high-volatility and directional environments, but requires further calibration and stability testing.

Microprice deviation contains measurable
