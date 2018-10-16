module.exports = function(wallaby) {
  return {
    files: [
      {
        pattern: "node_modules/babel-polyfill/dist/polyfill.js",
        instrument: false
      },
      "src/**/*.js",
      "!**/*.spec.js",
      "!node_modules/**/*.*"
    ],
    tests: ["**/*.spec.js"],
    env: {
      type: "node"
    },
    testFramework: "jest",
    compilers: {
      'src/**/*.js': wallaby.compilers.babel()
    },
    workers: {
      recycle: true
    },
    debug: true
  }
}
