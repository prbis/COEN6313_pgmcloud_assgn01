# plotDelays.py

import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Path to the delays JSON file
delays_file = 'delays_2024-10-16T00-43-17-063Z.json'  # Replace with your actual file name

# Load the delays data
with open(delays_file, 'r') as file:
    delays = json.load(file)

# Convert the delays dictionary to a DataFrame
data = []
for query, times in delays.items():
    for time in times:
        data.append({'Query': query, 'Delay_ms': time})

df = pd.DataFrame(data)

# Rename queries for better readability
query_names = {
    'query1': 'GetPrizesByCategory',
    'query2': 'CountLaureatesByCategoryAndYearRange',
    'query3': 'CountLaureatesByMotivationKeyword',
    'query4': 'GetLaureateDetailsByName'
}

df['Query'] = df['Query'].map(query_names)

# Set up the plotting style
sns.set(style='whitegrid', palette='muted')

# Plotting the box plots with enhancements
plt.figure(figsize=(14, 8))

# Create a more distinct color palette
palette = sns.color_palette("Set2")

# Draw the box plot
box = sns.boxplot(x='Query', y='Delay_ms', data=df, palette=palette, linewidth=2.5)

# Overlay a strip plot to show individual data points
sns.stripplot(x='Query', y='Delay_ms', data=df, color='black', alpha=0.6, jitter=True, size=5)

# Annotate each box plot with the median and quartiles
for i, patch in enumerate(box.artists):
    # Getting the median, lower quartile, and upper quartile
    query_name = list(query_names.values())[i]
    med = df[df['Query'] == query_name]['Delay_ms'].median()
    lower_quartile = df[df['Query'] == query_name]['Delay_ms'].quantile(0.25)
    upper_quartile = df[df['Query'] == query_name]['Delay_ms'].quantile(0.75)
    
    # Annotating the median
    plt.text(i, med, f'Median: {med:.2f}', horizontalalignment='center', size=12, color='black', weight='bold')
    
    # Annotating the lower and upper quartiles
    plt.text(i, lower_quartile, f'25%: {lower_quartile:.2f}', horizontalalignment='center', size=10, color='blue')
    plt.text(i, upper_quartile, f'75%: {upper_quartile:.2f}', horizontalalignment='center', size=10, color='blue')

# Customize axes and labels
plt.title('End-to-End Delay Distribution for Each gRPC Query')
plt.xlabel('gRPC Query')
plt.ylabel('Delay (ms)')
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Save and display the plot
plt.tight_layout()
plt.savefig('e2e_delay_boxplots_enhanced.png')
plt.show()
