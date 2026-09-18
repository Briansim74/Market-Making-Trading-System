# Market-Making Trading Engine
Research Findings, Live Results & Next Steps - 18 September 2026

## 1. Executive Summary
#### Strategy Status
The market-making engine has progressed from historical research and deterministic replay through paper/testnet execution to live capital deployment on Binance spot.

The current live deployment is focused on PEPEUSDT and is intentionally small. The current live run used approximately $100 of initial cash, a $1.10 base order size, and a maximum inventory of $2.50.

The current live run produced:
```
Net P&L:          -$0.04245
Fees paid:        $0.04065
Notional traded:  $40.65
Reported Sharpe:  -0.0209
```
The live sample is too small to draw conclusions about persistent profitability. Its primary value is validating whether the research and execution assumptions survive contact with real exchange executions.

### 1.1. Objective
I built a production-oriented market-making engine for systematic liquidity provision under realistic market-microstructure constraints.

The system combines:
- live L2 market data
- deterministic historical replay
- market discovery
- regime classification
- microstructure alpha
- structural fair-value estimation
- residual alpha
- toxicity modelling
- queue-aware execution simulation
- inventory-aware quoting
- exchange execution and reconciliation

The research and trading architecture deliberately separates signal generation from execution.

The objective is not simply to predict short-term price movements. It is to determine whether providing liquidity is economically attractive after accounting for:
- market regime
- adverse selection
- queue dynamics
- fees
- execution constraints
- inventory risk

The central research hypothesis is therefore:
```
Selective participation under realistic execution constraints is more important than maximizing predictive accuracy.
```

### 1.2. Key Research Findings
#### H1 - Market conditions differ materially across candidate markets.
The market-discovery layer evaluates spread, fees, liquidity, liquidity balance, trade activity and order-book depth.

PEPEUSDT currently provides a useful live research environment because it combines relatively wide displayed spreads with substantial displayed liquidity. However, the book is materially asymmetric, demonstrating that displayed spread and liquidity alone are not sufficient evidence of attractive market-making economics.

This led me to treat market selection as a first-stage filter rather than as evidence of trading edge.

#### H2 - Market regimes materially change the trading environment.
The GMM regime classifier uses spread, volatility, order imbalance, trade imbalance and microprice deviation to identify different market conditions.

The current regimes exhibit different future return and volatility characteristics. I therefore use regime classification primarily as a conditional control variable rather than as an independent alpha source.

The research question is not whether a regime predicts returns directly, but whether different regimes require different quoting, sizing and inventory policies.

#### H3 - Microprice contains predictive information beyond the immediate quote horizon.
The current out-of-sample microprice signal shows increasing predictive information through the tested horizons:
```
100ms → IC 0.129
500ms → IC 0.200
1s    → IC 0.273
5s    → IC 0.416
```

This suggests that microprice information may be more useful for short-horizon fair-value adjustment than for predicting the next tick.

The remaining question is whether incorporating the signal into reservation prices and quote skew improves realized market-making economics after fees, fills and adverse selection.

#### H4 - Live execution is exposing the gap between theoretical and realized market-making economics.
The initial live configuration currently uses the regime classifier but does not yet use the microprice-alpha, residual-alpha or toxicity models.

The live run produced -$0.04245 of P&L on $40.65 of traded notional, with $0.04065 of fees.

The result is too small to assess profitability, but it provides a baseline for measuring actual fills, maker/taker execution, inventory behaviour and realized markouts before adding further model complexity.

#### H5 - Current live fills show negative short-horizon markouts.
The current live fill sample shows negative signed markouts for both buy and sell executions across the measured horizons.

This is consistent with the hypothesis that the baseline strategy is exposed to adverse selection.

However, the sample is currently too small for statistical inference, and the markout instrumentation requires further validation because the current 100ms, 500ms and 1s outputs are identical.

The immediate priority is therefore to validate the measurement pipeline and collect a larger live-fill dataset before optimizing around the result.

#### H6 - The toxicity model is not yet validated.

The toxicity model is designed to estimate:
```
T(x) = E[future signed markout_h ∣ fill, state]
```
using spread, volatility, order imbalance, trade imbalance, microprice deviation and regime.

The current model produces NaN IC statistics and effectively constant predictions, so it cannot yet be treated as validated.

The next step is to build a sufficiently large and correctly labelled live-fill dataset, establish an out-of-sample evaluation framework, and then test whether toxicity predictions improve quote selection and sizing.

### 1.3. What I Learned
The main lesson from the project is:
```
Market making is not primarily a forecasting problem. It is a conditional execution problem.
```
A predictive signal only has economic value if it can be monetized through the execution mechanism.

I now separate the trading problem into:
```
Alpha
Where does potential edge exist?

Regime
When are the underlying signals reliable?

Toxicity
Is a prospective fill likely to be favorable or adverse?

Execution
Will the order actually receive a sufficiently favorable fill?

Inventory
Can repeated fills be managed without creating unacceptable directional exposure?
```

This decomposition has made the strategy more falsifiable.

A signal can have strong predictive statistics while producing little trading value if adverse selection, queue dynamics, fees or inventory effects consume the expected edge.

Live deployment has therefore shifted the research focus from unconditional price prediction toward execution-conditioned economics.

### 1.4. Current Research Priorities
The immediate research priorities are:
- Validate and recalibrate the regime classifier.
- Validate the live markout measurement pipeline.
- Accumulate a sufficiently large live-fill dataset.
- Test microprice alpha as a bounded fair-value and quote-skew adjustment.
- Train and validate the toxicity model out of sample.
- Determine whether toxicity predictions improve realized execution economics.
- Add residual alpha only if the existing signal stack leaves measurable unexplained information.
- Continue small-capital live validation before increasing risk.

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
 Queue + Execution
         ↓
  Inventory / Risk
         ↓
   Realized P&L
```

The objective is not simply to maximize predictive accuracy or backtested returns. It is to establish whether a market-making opportunity survives the complete chain:
```
Hypothesis → Measurement → Out-of-Sample Validation → Execution → Risk → Realized P&L
```

## 2. Research Objectives and Hypotheses
I separated the market-making system into independent hypotheses so that each component can be tested without attributing aggregate P&L to a single model.

#### Market-Making Edge
```
    Market Discovery / H1
            │
            ▼
    Regime Detection / H2
            │
            ▼
    Signal Generation
     ├─ Microstructure Alpha / H2
            │
            ▼
     Toxicity Filter / H4
            │
            ▼
    Quote Construction
            │
            ▼
    Execution Engine
            │
            ▼
Inventory & Risk Management
            │
            ▼
       Realized P&L
```
                     
| Hypothesis | Component        | Question	                                                                       | Primary Risk                            |
|------------|------------------|----------------------------------------------------------------------------------|-----------------------------------------|
| H1	     | Market Discovery | Does regime classification identify materially different execution environments? | Non-stationarity / misclassification    |
| H2	     | Regime           | Does regime classification identify materially different execution environments? | Non-stationarity / misclassification    |
| H3	     | Microprice alpha | Does microprice deviation contain useful short-horizon information?              | Signal decay / execution lag            |
| H4	     | Toxicity         | Can conditional markouts identify adverse-selection risk before quoting?         | Selection bias / insufficient fills     |

This separation allows each component to be evaluated independently rather than relying on aggregate live P&L.

## 4. Live Trading & Monitoring Dashboard
Real-time monitoring of inventory, quote state, fills, markouts, execution latency and P&L. The dashboard is used primarily for execution diagnostics and model validation rather than as a performance presentation.

#### Trading Dashboard
<img width="500" height="900" alt="dashboard" src="https://github.com/Briansim74/Market-Making-Trading-System/blob/main/dashboard.png"/>

The dashboard connects the research state at quote time to the eventual execution outcome, allowing live observations to be compared against simulated assumptions.

## 5. Live Trading & Execution Results

```
Live Since:         2026-09-03
Market:        	    PEPEUSDT
Venue:	            Binance Spot
Initial Cash:	    $100
Base Size:          $1.1
Maximum Inventory:  $2.5
Net P&L:            -$0.04245
Fees Paid:          $0.04065
P&L per fill:       -42449899.40 PEPE
Fees per fill:      40653199.40 PEPE
Notional Traded:    $40.65
Reported Sharpe:    -0.0209
```

#### P&L Curve
<img width="700" height="800" alt="pnl" src="https://github.com/Briansim74/Market-Making-Trading-System/blob/main/pnl_graph.png"/>

The live sample is too small to estimate persistent profitability.

The negative P&L should therefore not be interpreted as evidence that the complete strategy has negative expected return.

At the same time, it should not be ignored.

The current result provides evidence that the baseline execution configuration has not yet demonstrated that it can reliably convert displayed spread into realized economic value.

The most useful output from this run is therefore the execution dataset.

## 6. Empirical Findings
### 6.1. H1 - Market Discovery
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

### 6.2. H2 - Regime Classification
The regime classifier uses a Gaussian Mixture Model over:
- spread
- volatility
- order imbalance
- trade imbalance
- microprice deviation

#### Regime
<img width="800" height="400" alt="regime" src="https://github.com/Briansim74/Market-Making-Trading-System/blob/main/regime_detection.png"/>

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

### 6.3. H3 - Microprice Alpha
The microprice signal is:
```
micro_signal = microprice − mid
```

I tested the signal against future mid-price returns without incorporating the structural fair-value model.

#### Microprice IC Curve
<img width="400" height="800" alt="ic_curve" src="https://github.com/Briansim74/Market-Making-Trading-System/blob/main/microprice_rank_ic.png"/>

The results show increasing predictive information across the tested horizons.

The strongest result is at 5 seconds:
```
IC:           0.416
Rank IC:      0.330
Sharpe proxy: 0.207
```
The evidence suggests that the microprice signal contains information about subsequent price evolution.

However, this does not yet establish that the signal produces positive market-making returns.

The key question is now:
```
Does incorporating microprice information into reservation prices
and quote skew improve realized P&L after fees and adverse selection?
```
The next implementation will therefore be controlled.

Rather than allowing the model to directly determine trading direction, I will test microprice alpha as a bounded adjustment to:
- fair value
- reservation price
- quote skew
- order size

This preserves the distinction between prediction and execution policy.

### 6.4. H4 - Toxicity Modelling / Live Markout Analysis
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
     Queue
       ↓
     Fill
       ↓
Future Price Path
       ↓
Signed Markout
```
This is more informative than evaluating fills only through aggregate P&L.

#### Markouts
<img width="500" height="900" alt="markouts" src="https://github.com/Briansim74/Market-Making-Trading-System/blob/main/signed_markouts.png"/>

The current live sample shows negative markouts for both buy and sell fills, particularly at short horizons. This is consistent with the hypothesis that the strategy is currently exposed to adverse selection.

However, the current dataset is not yet large enough for statistical inference. The immediate task is therefore to improve the measurement infrastructure before optimizing the model around it.

#### 6.4.1. Toxicity Model
The toxicity model is intended to predict:
```
E[ Markout_h ∣ Fill, State]
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
```
```
           Neutral
              ↓
       Normal quoting
```
```
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

## 7. What I Learned
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

| Source of edge                  | Question                                  |
|---------------------------------|-------------------------------------------|
| Market discovery                | Where should I trade?                     |
| Regime classification           | When should I provide liquidity?          |
| Structural and Microprice alpha | Where should fair value be?               |
| Toxicity                        | Is a fill likely to be favorable?         |
| Queue and execution modelling   | Will I actually get the fill?             |
| Inventory and risk management   | Will the strategy survive repeated fills? |

This decomposition also makes failure easier to diagnose.

If the strategy loses money, I can ask whether the problem came from:
- incorrect fair value
- incorrect regime classification
- adverse selection
- queue estimation
- fees
- inventory
- execution logic

That is more useful than simply optimizing aggregate P&L.

## 8. Next Steps
#### 8.1. Recalibrate the Regime Classifier
The current GMM provides a useful first segmentation, but the regime boundaries need to be tested for stability.

I will:
- recompute regime distributions over additional historical periods
- evaluate cluster stability
- examine transition probabilities
- test regime-specific alpha decay
- compare future volatility and markouts across regimes
- determine whether regime-specific quoting policies improve execution

The goal is to establish whether the regime classifier provides stable conditional information rather than simply fitting the current snapshot distribution.

#### 8.2. Add Microprice Alpha to the Quoting Policy
The current microprice results justify testing the signal as an additional fair-value component.

The initial implementation will be conservative:
```
       Mid
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
Regime → Mid → Quote
```
against:

Enhanced
```
Regime → Mid + Microprice Alpha → Quote
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

#### 8.3. Build the Toxicity Dataset
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

#### 8.4. Validate Toxicity Out of Sample
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

#### 8.5. Evaluate Residual Alpha Only If Necessary
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

#### 8.6. Improve Queue Modelling
Live deployment provides data that cannot be fully reproduced through historical snapshots.

I will use live execution to estimate:
- queue position
- trade-driven depletion
- cancellation-driven depletion
- quote churn
- snapshot staleness
- time-to-fill distributions

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

## 9. Falsification Criteria
The purpose of the research process is to allow the strategy to fail.

#### 9.1. Regime Model
The regime hypothesis will be weakened if:
- cluster definitions are unstable across periods;
- regimes do not produce materially different execution characteristics;
- regime-conditioned policies do not improve out-of-sample economics;
- results depend heavily on arbitrary feature scaling or initialization.

#### 9.2. Microprice Alpha
The microprice hypothesis will be weakened if:
- predictive information disappears out of sample;
- the signal does not improve fair-value estimation;
- improvements in IC do not translate into execution economics;
- transaction costs and adverse selection consume the signal;
- performance is highly sensitive to a small number of periods.

#### 9.3. Toxicity Model
The toxicity hypothesis will be weakened if:
- predicted toxicity does not separate realized markouts;
- model performance disappears out of sample;
- filtering toxic fills reduces participation more than it improves economics;
- the model does not improve realized P&L after accounting for missed fills.

#### 9.4. Overall Strategy
The market-making strategy will be weakened if:
- displayed spreads consistently fail to translate into realized spread capture;
- adverse selection dominates spread capture;
- queue and latency costs consume the available edge;
- inventory management materially reduces expected returns;
- performance disappears after realistic fees and execution costs.

## 10. Research Framework Going Forward
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

## 11. Conclusion
The market-making system has progressed from historical research and deterministic replay to TestNet execution and live capital deployment. The current live sample is still primarily an execution-validation dataset, rather than sufficient evidence of statistically significant persistent profitability.

The research has nevertheless produced several useful findings.

First, microstructure information contains predictive content beyond the immediate quote horizon. The current microprice signal shows increasing IC through the tested 5-second horizon, suggesting that the information may be more useful for short-horizon fair-value adjustment than for purely ultra-fast prediction.

Second, market-making edge is conditional. The usefulness of a signal depends on the liquidity regime, toxicity of incoming flow, inventory state and execution conditions under which the quote is provided.

Third, execution is a first-class component of the strategy rather than an implementation detail. Queue position, cancellation dynamics, latency, snapshot age and fill selection can determine whether a statistically useful signal becomes economically valuable.

Fourth, live deployment provides information that historical backtesting cannot. The current objective is to measure the gap between simulated and realized:
- fill probability
- queue depletion
- latency
- quote persistence
- toxicity
- markout
- inventory behaviour

The research framework is therefore:
```
             Hypothesis
                  ↓
            Measurement
                  ↓
        Out-of-Sample Test
                  ↓
           Execution Model
                  ↓
             Simulation
                  ↓
          Limited Live Test
                  ↓
        Execution Diagnostics
                  ↓
          Model Calibration
                  ↓
          Realized Economics
                  ↓
               Risk
                  ↓
              Scaling
```
The central research question has evolved from:
```
Can I predict short-term price movements?
```
to:
```
Under what conditions is providing liquidity economically valuable,
and can that value survive adverse selection, queue dynamics, latency, fees and inventory risk?
```
The next stage is therefore focused on calibrating the live execution model, validating toxicity and fill predictions against realized executions, decomposing live P&L, and establishing explicit criteria for scaling or rejecting individual components of the strategy.

The objective is not simply to demonstrate that the strategy can generate attractive backtest results. It is to determine whether the apparent edge remains robust when exposed to the actual mechanics and constraints of live market making.
