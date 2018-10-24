module.exports = {
  insert: jest.fn().mockImplementation(insert => Promise.resolve())
}