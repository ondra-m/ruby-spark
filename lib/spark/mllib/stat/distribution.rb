##
# MultivariateGaussian
#
# This class provides basic functionality for a Multivariate Gaussian (Normal) Distribution. In
# the event that the covariance matrix is singular, the density will be computed in a
# reduced dimensional subspace under which the distribution is supported.
#
# == Arguments:
# mu:: The mean vector of the distribution
# sigma:: The covariance matrix of the distribution
#
Spark::Mllib::MultivariateGaussian = Struct.new(:mu, :sigma)
