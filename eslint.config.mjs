// ioBroker eslint template configuration file for js and ts files
import config from '@iobroker/eslint-config';

export default [
    ...config,
    {
        // files excluded from linting
        ignores: [
            '.dev-server/',
            '.vscode/',
            '.claude/',
            '*.test.js',
            'test/**/*.js',
            '*.config.mjs',
            'build',
            'dist',
            'node_modules',
        ],
    },
];
