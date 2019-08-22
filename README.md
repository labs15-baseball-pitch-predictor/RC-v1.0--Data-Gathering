# RC-v1.0--Data-Gathering
## Intial data gathering for the project


### Clone the Repo

<pre><code>!git clone https://github.com/labs15-baseball-pitch-predictor/RC-v1.0--Data-Gathering</code></pre>


### Take a peek at the directory 

<pre><code>!ls RC-v1.0--Data-Gathering/season_pickles</code></pre>


### Load your desired season

<pre><code>import pandas as pd
path = "RC-v1.0--Data-Gathering/season_pickles/pitches_2017.pkl"
df_17 = pd.read_pickle(path, compression='zip')</code></pre>
