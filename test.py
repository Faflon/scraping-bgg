import pandas as pd

# Load the dataset
df = pd.read_csv('data/final_dataset.csv')

# 1. Drop all the NaNs (the games that didn't win)
winners = df.dropna(subset=['Spiel_des_Jahres'])

# 2. Filter out the literal string 'None' just in case it saved as text
winners = winners[winners['Spiel_des_Jahres'] != 'None']

print(f"Total Spiel des Jahres winners in the Top 1000: {len(winners)}\n")

if len(winners) > 0:
    print(winners[['Rank', 'Title', 'Year', 'Spiel_des_Jahres']].head(20).to_string(index=False))