from argparse import ArgumentParser
import torch
import pytorch_lightning as pl
from torch.nn import functional as F
from torch.utils.data import DataLoader, random_split
from clearml import Task

from torchvision.datasets.mnist import MNIST
from torchvision import transforms


class LitClassifier(pl.LightningModule):
    def __init__(self, hidden_dim=128, learning_rate=1e-3):
        super().__init__()
        self.save_hyperparameters()

        self.l1 = torch.nn.Linear(28 * 28, self.hparams.hidden_dim)
        self.l2 = torch.nn.Linear(self.hparams.hidden_dim, 10)

    def forward(self, x):
        x = x.view(x.size(0), -1)
        x = torch.relu(self.l1(x))
        x = torch.relu(self.l2(x))
        return x

    def training_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)
        loss = F.cross_entropy(y_hat, y)
        return loss

    def validation_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)
        loss = F.cross_entropy(y_hat, y)
        self.log("valid_loss", loss)

    def test_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)
        loss = F.cross_entropy(y_hat, y)
        self.log("test_loss", loss)

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=self.hparams.learning_rate)

    @staticmethod
    def add_model_specific_args(parent_parser):
        parser = ArgumentParser(parents=[parent_parser], add_help=False)
        parser.add_argument("--hidden_dim", type=int, default=128)
        parser.add_argument("--learning_rate", type=float, default=0.0001)
        return parser


def process():
    pl.seed_everything(0)
    dataset = MNIST("", train=True, download=True, transform=transforms.ToTensor())
    mnist_test = MNIST("", train=False, download=True, transform=transforms.ToTensor())
    mnist_train, mnist_val = random_split(dataset, [55000, 5000])
    train_loader = DataLoader(mnist_train, batch_size=32)
    val_loader = DataLoader(mnist_val, batch_size=32)
    test_loader = DataLoader(mnist_test, batch_size=32)
    model = LitClassifier(128, 0.0001)
    trainer = pl.Trainer(max_epochs=3)
    trainer.fit(model, train_loader, val_loader)
    trainer.test(test_dataloaders=test_loader)
    return "Test"
