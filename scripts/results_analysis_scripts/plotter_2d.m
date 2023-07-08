path = input('Insert the test to plot: ', 's');

% File CSV di input
basePath = '../data/';
csvFiles = {strcat(basePath, path, '/dataset_test.csv'), strcat(basePath, path, '/centroids_test.csv'), strcat(basePath, path, '/results.csv')};

% Carica i dati dai file CSV
data = cell(1, 3);
for i = 1:3
    data{i} = readmatrix(csvFiles{i});
end

% Crea una figura
figure

% Traccia i dati
plot(data{1}(:, 1), data{1}(:, 2), 'bo', 'MarkerFaceColor', 'b')
hold on
plot(data{2}(:, 2), data{2}(:, 3), 'g^', 'MarkerFaceColor', 'g')
plot(data{3}(:, 1), data{3}(:, 2), 'rs', 'MarkerFaceColor', 'r')

% Aggiungi etichette e titolo al plot
xlabel('X');
ylabel('Y');
title('Plot 2D dei punti');

% Mostra la griglia
grid on;

% Visualizza una legenda
legend('dataset_test', 'centroids_test', 'results');