import pandas as pd
import re

def normalize_title(title):
    # This function cleans the titles to ensure a high match rate between BGG and Wikipedia
    if pd.isna(title):
        return ""
    # Remove all non-alphanumeric characters and convert to lowercase
    clean = re.sub(r'[^a-zA-Z0-9\s]', '', str(title)).lower()
    # Remove extra spaces
    return " ".join(clean.split())

def merge_project_data():
    print("Loading data files...")
    try:
        # Load the three datasets
        raw_df = pd.read_csv('data/raw_urls.csv')
        meta_df = pd.read_csv('data/metadata.csv')
        awards_df = pd.read_csv('data/awards.csv')
    except FileNotFoundError as e:
        print(f"Error loading files. Ensure all previous scripts have been run. Details: {e}")
        return

    print("Merging BGG datasets...")
    # 1. Merge Phase 1 (Raw) and Phase 2 (Metadata)
    # We use a left merge on the URL to ensure we keep all 1000 games even if metadata failed for a few
    bgg_df = pd.merge(raw_df, meta_df, left_on='URL', right_on='url', how='left')
    
    # Drop the duplicate 'url' column coming from the metadata dataframe
    if 'url' in bgg_df.columns:
        bgg_df.drop('url', axis=1, inplace=True)

    print("Normalizing titles for Entity Resolution...")
    # 2. Create temporary columns for merging with Wikipedia data
    bgg_df['norm_Title'] = bgg_df['Title'].apply(normalize_title)
    awards_df['norm_Title'] = awards_df['Title'].apply(normalize_title)

    print("Merging Wikipedia awards data...")
    # 3. Merge the combined BGG data with the Wikipedia awards data
    final_df = pd.merge(bgg_df, awards_df, on='Title', how='left')

    # 4. Clean up the final dataset
    # Fill missing award statuses with 'None' instead of NaN
    final_df['Spiel_des_Jahres'] = final_df['Spiel_des_Jahres'].fillna('None')
    
    # Drop the temporary merging columns and redundant Wikipedia title column
    final_df.drop(['norm_Title_x', 'norm_Title_y'], axis=1, inplace=True)

    # 5. Export the final master dataset
    output_path = 'data/final_dataset.csv'
    final_df.to_csv(output_path, index=False)
    print(f"SUCCESS: Master dataset created with {len(final_df)} rows and {len(final_df.columns)} columns.")
    print(f"Saved to: {output_path}")

if __name__ == "__main__":
    merge_project_data()